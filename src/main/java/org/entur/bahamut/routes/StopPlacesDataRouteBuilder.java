package org.entur.bahamut.routes;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.entur.bahamut.routes.json.PeliasDocument;
import org.entur.bahamut.routes.mapper.StopPlaceBoostConfiguration;
import org.entur.bahamut.routes.mapper.StopPlaceToPeliasMapper;
import org.entur.bahamut.routes.mapper.TopographicPlaceToPeliasMapper;
import org.entur.bahamut.services.BahamutBlobStoreService;
import org.entur.bahamut.services.KakkaBlobStoreService;
import org.entur.netex.NetexParser;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.rutebanken.netex.model.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.entur.bahamut.services.BlobStoreService.FILE_HANDLE;

@Component
public class StopPlacesDataRouteBuilder extends RouteBuilder {

    @Value("${bahamut.camel.redelivery.max:3}")
    private int maxRedelivery;

    @Value("${bahamut.camel.redelivery.delay:5000}")
    private int redeliveryDelay;

    @Value("${bahamut.camel.redelivery.backoff.multiplier:3}")
    private int backOffMultiplier;

    @Value("${pelias.update.cron.schedule:0+0/2+*+1/1+*+?+*}")
    private String cronSchedule;

    @Value("${blobstore.gcs.kakka.tiamat.geocoder.file:tiamat/geocoder/tiamat_export_geocoder_latest.zip}")
    private String tiamatGeocoderFile;

    @Value("${blobstore.gcs.bahamut.geocoder.file:tiamat_export_geocoder_latest.zip}")
    private String bahamutGeocoderFile;

    @Value("${bahamut.workdir:/tmp/tiamat/geocoder}")
    private String bahamutWorkDir;

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
    public void configure() throws Exception {

        errorHandler(defaultErrorHandler()
                .redeliveryDelay(redeliveryDelay)
                .maximumRedeliveries(maxRedelivery)
                .onRedelivery(this::logRedelivery)
                .useExponentialBackOff()
                .backOffMultiplier(backOffMultiplier)
                .logExhausted(true)
                .logRetryStackTrace(true));

        from("quartz://bahamut/peliasUpdate?cron=" + cronSchedule + "&trigger.timeZone=Europe/Oslo")
                .process(exchange -> {
                    Message in = exchange.getIn();
                })
                .setHeader(FILE_HANDLE, constant(tiamatGeocoderFile))
                .bean(kakkaBlobStoreService, "getBlob")
                .process(exchange -> {
                    Message in = exchange.getIn();
                })
                .setHeader("bahamutWorkDir", constant(bahamutWorkDir))
                .process(StopPlacesDataRouteBuilder::unzipFile)
                .process(StopPlacesDataRouteBuilder::parseNetexFile)
                .process(this::fromDeliveryPublicationStructure)
                .bean(new CSVCreator())
                .setHeader(FILE_HANDLE, constant(bahamutGeocoderFile))
                .process(StopPlacesDataRouteBuilder::zipFile)
                .process(exchange -> {
                    Message in = exchange.getIn();
                })
                .bean(bahamutBlobStoreService, "uploadBlob");
    }

    protected void logRedelivery(Exchange exchange) {
        int redeliveryCounter = exchange.getIn().getHeader("CamelRedeliveryCounter", Integer.class);
        int redeliveryMaxCounter = exchange.getIn().getHeader("CamelRedeliveryMaxCounter", Integer.class);
        Throwable camelCaughtThrowable = exchange.getProperty("CamelExceptionCaught", Throwable.class);

        log.warn("Exchange failed, redelivering the message locally, attempt {}/{}...", redeliveryCounter, redeliveryMaxCounter, camelCaughtThrowable);
    }

    private static void parseNetexFile(Exchange exchange) {
        var parser = new NetexParser();
        try (Stream<Path> paths = Files.walk(Paths.get(exchange.getIn().getHeader("bahamutWorkDir", String.class)))) {
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

    public static void unzipFile(Exchange exchange) {
        InputStream inputStream = exchange.getIn().getBody(InputStream.class);
        String targetFolder = exchange.getIn().getHeader("bahamutWorkDir", String.class);
        try (ZipInputStream zis = new ZipInputStream(inputStream)) {
            byte[] buffer = new byte[1024];
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                String fileName = zipEntry.getName();
//                logger.info("unzipping file {} in folder {} ", fileName, targetFolder);

                Path path = Path.of(targetFolder + "/" + fileName);
                if (Files.isDirectory(path)) {
                    path.toFile().mkdirs();
                    continue;
                }

                File parent = path.toFile().getParentFile();
                if (parent != null) {
                    parent.mkdirs();
                }


                FileOutputStream fos = new FileOutputStream(path.toFile());
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
        } catch (IOException ioE) {
            throw new RuntimeException("Unzipping archive failed: " + ioE.getMessage(), ioE);
        }
    }

    public static void zipFile(Exchange exchange) {
        InputStream inputStream = exchange.getIn().getBody(InputStream.class);
        try {
            byte[] inputBytes = inputStream.readAllBytes();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ZipOutputStream zos = new ZipOutputStream(baos);
            ZipEntry entry = new ZipEntry("tiamat_csv_export_geocoder_latest.csv");
            entry.setSize(inputBytes.length);
            zos.putNextEntry(entry);
            zos.write(inputBytes);
            zos.closeEntry();
            zos.close();
            exchange.getIn().setBody(new ByteArrayInputStream(baos.toByteArray()));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to add file to zip: " + ex.getMessage(), ex);
        }
    }

    private void fromDeliveryPublicationStructure(Exchange exchange) {

        NetexEntitiesIndex netexEntitiesIndex = exchange.getIn().getBody(NetexEntitiesIndex.class);

        StopPlaceToPeliasMapper mapper = new StopPlaceToPeliasMapper(stopPlaceBoostConfiguration);

        List<ElasticsearchCommand> stopPlaceCommands = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getStopPlaces().getStopPlace())
                .flatMap(stopPlaces -> toPlaceHierarchies(stopPlaces).stream())
                .map(mapper::toPeliasDocuments)
                .flatMap(Collection::stream)
                .sorted(new PeliasDocumentPopularityComparator()).map(ElasticsearchCommand::peliasIndexCommand).toList();

        List<ElasticsearchCommand> topographicalPlaceCommands = netexEntitiesIndex.getSiteFrames().stream()
                .map(siteFrame -> siteFrame.getTopographicPlaces().getTopographicPlace())
                .flatMap(topographicPlaces -> addTopographicPlaceCommands(topographicPlaces).stream()).toList();

        exchange.getIn().setBody(
                Stream.concat(Stream.of(stopPlaceCommands), Stream.of(topographicalPlaceCommands)).flatMap(List::stream).toList()
        );
    }

    private static List<ElasticsearchCommand> addTopographicPlaceCommands(List<TopographicPlace> places) {
        if (!CollectionUtils.isEmpty(places)) {
            TopographicPlaceToPeliasMapper mapper = new TopographicPlaceToPeliasMapper(1L, Collections.emptyList());
            return places.stream()
                    .map(p -> mapper.toPeliasDocuments(new PlaceHierarchy<>(p)))
                    .flatMap(Collection::stream)
                    .sorted(new PeliasDocumentPopularityComparator())
                    .filter(Objects::nonNull)
                    .map(ElasticsearchCommand::peliasIndexCommand)
                    .collect(Collectors.toList());
        }
        return new ArrayList<>();
    }


    private static class PeliasDocumentPopularityComparator implements Comparator<PeliasDocument> {

        @Override
        public int compare(PeliasDocument o1, PeliasDocument o2) {
            Long p1 = o1 == null || o1.getPopularity() == null ? 1L : o1.getPopularity();
            Long p2 = o2 == null || o2.getPopularity() == null ? 1L : o2.getPopularity();
            return -p1.compareTo(p2);
        }
    }

    /**
     * Map list of stop places to list of hierarchies.
     */
    private static Set<PlaceHierarchy<StopPlace>> toPlaceHierarchies(List<StopPlace> places) {
        Map<String, List<StopPlace>> childrenByParentIdMap = places.stream().filter(sp -> sp.getParentSiteRef() != null).collect(Collectors.groupingBy(sp -> sp.getParentSiteRef().getRef()));
        Set<PlaceHierarchy<StopPlace>> allStopPlaces = new HashSet<>();
        expandStopPlaceHierarchies(places.stream().filter(sp -> sp.getParentSiteRef() == null).map(sp -> createHierarchyForStopPlace(sp, null, childrenByParentIdMap)).collect(Collectors.toList()), allStopPlaces);
        return allStopPlaces;
    }

    private static void expandStopPlaceHierarchies(Collection<PlaceHierarchy<StopPlace>> hierarchies, Set<PlaceHierarchy<StopPlace>> target) {
        if (hierarchies != null) {
            for (PlaceHierarchy<StopPlace> stopPlacePlaceHierarchy : hierarchies) {
                target.add(stopPlacePlaceHierarchy);
                expandStopPlaceHierarchies(stopPlacePlaceHierarchy.getChildren(), target);
            }
        }
    }

    private static PlaceHierarchy<StopPlace> createHierarchyForStopPlace(StopPlace stopPlace, PlaceHierarchy<StopPlace> parent, Map<String, List<StopPlace>> childrenByParentIdMap) {
        List<StopPlace> children = childrenByParentIdMap.get(stopPlace.getId());
        List<PlaceHierarchy<StopPlace>> childHierarchies = new ArrayList<>();
        PlaceHierarchy<StopPlace> hierarchy = new PlaceHierarchy<>(stopPlace, parent);
        if (children != null) {
            childHierarchies = children.stream().map(child -> createHierarchyForStopPlace(child, hierarchy, childrenByParentIdMap)).collect(Collectors.toList());
        }
        hierarchy.setChildren(childHierarchies);
        return hierarchy;
    }
}