package org.entur.bahamut.camel.routes;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.entur.bahamut.camel.PeliasIndexParentInfoEnricher;
import org.entur.bahamut.camel.ZipUtilities;
import org.entur.bahamut.camel.adminUnitsRepository.AdminUnitsCache;
import org.entur.bahamut.camel.csv.CSVCreator;
import org.entur.bahamut.camel.placeHierarchy.PlaceHierarchies;
import org.entur.bahamut.camel.placeHierarchy.PlaceHierarchy;
import org.entur.bahamut.camel.routes.json.PeliasDocument;
import org.entur.bahamut.camel.routes.mapper.StopPlaceBoostConfiguration;
import org.entur.bahamut.camel.routes.mapper.StopPlaceToPeliasMapper;
import org.entur.bahamut.camel.routes.mapper.TopographicPlaceToPeliasMapper;
import org.entur.bahamut.services.BahamutBlobStoreService;
import org.entur.bahamut.services.KakkaBlobStoreService;
import org.entur.netex.NetexParser;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

import static org.entur.bahamut.camel.PeliasIndexParentInfoEnricher.ADMIN_UNITS_CACHE_PROPERTY;
import static org.entur.bahamut.services.BlobStoreService.FILE_HANDLE;

@Component
public class StopPlacesDataRouteBuilder extends RouteBuilder {

    public static final String WORK_DIRECTORY_HEADER = "bahamutWorkDir";

    @Value("${bahamut.camel.redelivery.max:3}")
    private int maxRedelivery;

    @Value("${bahamut.camel.redelivery.delay:5000}")
    private int redeliveryDelay;

    @Value("${bahamut.camel.redelivery.backoff.multiplier:3}")
    private int backOffMultiplier;

    @Value("${pelias.update.cron.schedule:0+0/3+*+1/1+*+?+*}")
    private String cronSchedule;

    @Value("${blobstore.gcs.kakka.tiamat.geocoder.file:tiamat/geocoder/tiamat_export_geocoder_latest.zip}")
    private String tiamatGeocoderFile;

    @Value("${blobstore.gcs.bahamut.geocoder.file:tiamat_export_geocoder_latest.zip}")
    private String bahamutGeocoderFile;

    @Value("${bahamut.workdir:/tmp/bahamut/geocoder}")
    private String bahamutWorkDir;

    @Value("${admin.units.cache.max.size:30000}")
    private Integer cacheMaxSize;

    private final StopPlaceBoostConfiguration stopPlaceBoostConfiguration;
    private final KakkaBlobStoreService kakkaBlobStoreService;
    private final BahamutBlobStoreService bahamutBlobStoreService;

    public StopPlacesDataRouteBuilder(
            StopPlaceBoostConfiguration stopPlaceBoostConfiguration,
            KakkaBlobStoreService kakkaBlobStoreService,
            BahamutBlobStoreService bahamutBlobStoreService) {
        this.stopPlaceBoostConfiguration = stopPlaceBoostConfiguration;
        this.kakkaBlobStoreService = kakkaBlobStoreService;
        this.bahamutBlobStoreService = bahamutBlobStoreService;
    }

    @Override
    public void configure() {

        errorHandler(defaultErrorHandler()
                .redeliveryDelay(redeliveryDelay)
                .maximumRedeliveries(maxRedelivery)
                .onRedelivery(this::logRedelivery)
                .useExponentialBackOff()
                .backOffMultiplier(backOffMultiplier)
                .logExhausted(true)
                .logRetryStackTrace(true));

        from("quartz://bahamut/makeCSV?cron=" + cronSchedule + "&trigger.timeZone=Europe/Oslo")
                .to("direct:makeCSV");

        from("direct:makeCSV")
                .process(exchange -> {
                    Message in = exchange.getIn();
                })
                .setHeader(FILE_HANDLE, constant(tiamatGeocoderFile))
                .bean(kakkaBlobStoreService, "getBlob")
                .setHeader(WORK_DIRECTORY_HEADER, constant(bahamutWorkDir))
                .process(ZipUtilities::unzipFile)
                .process(StopPlacesDataRouteBuilder::parseNetexFile)
                .process(this::buildAdminUnitCache)
                .process(this::netexEntitiesIndexToPeliasDocument)
                .bean(new PeliasIndexParentInfoEnricher())
                .bean(new CSVCreator())
                .process(ZipUtilities::zipFile)
                .setHeader(FILE_HANDLE, constant(bahamutGeocoderFile))
                .bean(bahamutBlobStoreService, "uploadBlob");
    }

    private void logRedelivery(Exchange exchange) {
        int redeliveryCounter = exchange.getIn().getHeader("CamelRedeliveryCounter", Integer.class);
        int redeliveryMaxCounter = exchange.getIn().getHeader("CamelRedeliveryMaxCounter", Integer.class);
        Throwable camelCaughtThrowable = exchange.getProperty("CamelExceptionCaught", Throwable.class);

        log.warn("Exchange failed, redelivering the message locally, attempt {}/{}...", redeliveryCounter, redeliveryMaxCounter, camelCaughtThrowable);
    }

    private static void parseNetexFile(Exchange exchange) {
        var parser = new NetexParser();
        try (Stream<Path> paths = Files.walk(Paths.get(exchange.getIn().getHeader(WORK_DIRECTORY_HEADER, String.class)))) {
            paths.filter(Files::isRegularFile).findFirst().ifPresent(path -> {
                try (InputStream inputStream = new FileInputStream(path.toFile())) {
                    exchange.getIn().setBody(parser.parse(inputStream));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void netexEntitiesIndexToPeliasDocument(Exchange exchange) {

        NetexEntitiesIndex netexEntitiesIndex = exchange.getIn().getBody(NetexEntitiesIndex.class);

        StopPlaceToPeliasMapper stopPlaceToPeliasMapper = new StopPlaceToPeliasMapper(stopPlaceBoostConfiguration);
        TopographicPlaceToPeliasMapper topographicPlaceToPeliasMapper = new TopographicPlaceToPeliasMapper(1L, Collections.emptyList());

        List<PeliasDocument> stopPlaceCommands = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getStopPlaces().getStopPlace())
                .flatMap(stopPlaces -> PlaceHierarchies.create(stopPlaces).stream())
                .flatMap(placeHierarchies -> stopPlaceToPeliasMapper.toPeliasDocuments(placeHierarchies).stream())
                .sorted(new PeliasDocumentPopularityComparator())
                .toList();

        List<PeliasDocument> topographicalPlaceCommands = netexEntitiesIndex.getSiteFrames().stream()
                .flatMap(siteFrame -> siteFrame.getTopographicPlaces().getTopographicPlace().stream())
                .flatMap(topographicPlace -> topographicPlaceToPeliasMapper.toPeliasDocuments(new PlaceHierarchy<>(topographicPlace)).stream())
                .sorted(new PeliasDocumentPopularityComparator())
                .toList();

        exchange.getIn().setBody(
                Stream.concat(Stream.of(stopPlaceCommands), Stream.of(topographicalPlaceCommands))
                        .flatMap(List::stream)
                        .filter(PeliasIndexValidCommandFilter::isValid)
                        .toList()
        );
    }

    private static class PeliasDocumentPopularityComparator implements Comparator<PeliasDocument> {

        @Override
        public int compare(PeliasDocument o1, PeliasDocument o2) {
            Long p1 = o1 == null || o1.getPopularity() == null ? 1L : o1.getPopularity();
            Long p2 = o2 == null || o2.getPopularity() == null ? 1L : o2.getPopularity();
            return -p1.compareTo(p2);
        }
    }

    private void buildAdminUnitCache(Exchange exchange) {
        NetexEntitiesIndex netexEntitiesIndex = exchange.getIn().getBody(NetexEntitiesIndex.class);
        AdminUnitsCache adminUnitsCache = AdminUnitsCache.buildNewCache(netexEntitiesIndex, cacheMaxSize);
        exchange.setProperty(ADMIN_UNITS_CACHE_PROPERTY, adminUnitsCache);
    }
}