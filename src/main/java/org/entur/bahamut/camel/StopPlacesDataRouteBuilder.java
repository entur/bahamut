package org.entur.bahamut.camel;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.entur.bahamut.adminUnitsCache.AdminUnitsCache;
import org.entur.bahamut.csv.CSVCreator;
import org.entur.bahamut.peliasDocument.stopPlacestoPeliasDocument.StopPlacesToPeliasDocument;
import org.entur.bahamut.services.BahamutBlobStoreService;
import org.entur.bahamut.services.KakkaBlobStoreService;
import org.entur.netex.NetexParser;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.entur.bahamut.services.BlobStoreService.BLOB_STORE_FILE_HANDLE;

@Component
public class StopPlacesDataRouteBuilder extends RouteBuilder {

    private static final Logger logger = LoggerFactory.getLogger(StopPlacesDataRouteBuilder.class);

    public static final String WORK_DIRECTORY_HEADER = "bahamutWorkDir";
    public static final String OUTPUT_FILENAME_HEADER = "bahamutOutputFilename";
    public static final String ADMIN_UNITS_CACHE_PROPERTY = "AdminUnitsCache";

    @Value("${bahamut.camel.redelivery.max:3}")
    private int maxRedelivery;

    @Value("${bahamut.camel.redelivery.delay:5000}")
    private int redeliveryDelay;

    @Value("${bahamut.camel.redelivery.backoff.multiplier:3}")
    private int backOffMultiplier;

    @Value("${bahamut.update.cron.schedule:0+0/1+*+1/1+*+?+*}")
    private String cronSchedule;

    @Value("${blobstore.gcs.kakka.tiamat.geocoder.file:tiamat/geocoder/tiamat_export_geocoder_latest.zip}")
    private String tiamatGeocoderFile;

    @Value("${blobstore.gcs.kakka.sweden.geocoder.file:sweden/geocoder/sweden.zip}")
    private String swedenGeocoderFile;

    @Value("${bahamut.workdir:/tmp/bahamut/geocoder}")
    private String bahamutWorkDir;

    @Value("${admin.units.cache.max.size:30000}")
    private Integer cacheMaxSize;

    private final KakkaBlobStoreService kakkaBlobStoreService;
    private final BahamutBlobStoreService bahamutBlobStoreService;
    private final StopPlacesToPeliasDocument stopPlacesToPeliasDocument;

    public StopPlacesDataRouteBuilder(
            KakkaBlobStoreService kakkaBlobStoreService,
            BahamutBlobStoreService bahamutBlobStoreService,
            StopPlacesToPeliasDocument stopPlacesToPeliasDocument) {
        this.kakkaBlobStoreService = kakkaBlobStoreService;
        this.bahamutBlobStoreService = bahamutBlobStoreService;
        this.stopPlacesToPeliasDocument = stopPlacesToPeliasDocument;
    }

    private static final AtomicBoolean readyToProcess = new AtomicBoolean(true);

    public static boolean readyToProcess() {
        boolean readyToProcess = StopPlacesDataRouteBuilder.readyToProcess.get();
        if (readyToProcess) {
            StopPlacesDataRouteBuilder.readyToProcess.set(false);
        }
        return readyToProcess;
    }


    @Override
    public void configure() {

        errorHandler(defaultErrorHandler()
                .redeliveryDelay(redeliveryDelay)
                .maximumRedeliveries(maxRedelivery)
                .onRedelivery(StopPlacesDataRouteBuilder::logRedelivery)
                .useExponentialBackOff()
                .backOffMultiplier(backOffMultiplier)
                .logExhausted(true)
                .logRetryStackTrace(true));

        from("quartz://bahamut/makeCSV?cron=" + cronSchedule + "&trigger.timeZone=Europe/Oslo")
                .to("direct:makeCSV");

        from("direct:makeCSV")
                .filter(method(StopPlacesDataRouteBuilder.class, "readyToProcess"))
                .to("direct:cacheAdminUnits")
                .setHeader(BLOB_STORE_FILE_HANDLE, constant(swedenGeocoderFile))
                .bean(kakkaBlobStoreService, "getBlob")
                .setHeader(WORK_DIRECTORY_HEADER, constant(bahamutWorkDir))
                .process(ZipUtilities::unzipFile)
                .process(StopPlacesDataRouteBuilder::parseNetexFile)
                .process(this::netexEntitiesIndexToPeliasDocument)
                .bean(new CSVCreator())
                .process(StopPlacesDataRouteBuilder::setOutputFilenameHeaders)
                .process(ZipUtilities::zipFile)
                .bean(bahamutBlobStoreService, "uploadBlob")
                .process(StopPlacesDataRouteBuilder::createCurrentFile)
                .bean(bahamutBlobStoreService, "uploadBlob");

        from("direct:cacheAdminUnits")
                .setHeader(BLOB_STORE_FILE_HANDLE, constant(tiamatGeocoderFile))
                .bean(kakkaBlobStoreService, "getBlob")
                .process(this::unzipStopPlacesToWorkingDirectory)
                .process(this::parseStopPlacesNetexFile)
                .process(this::buildAdminUnitCache);
    }

    private void unzipStopPlacesToWorkingDirectory(Exchange exchange) {
        logger.debug("Unzipping stop places file");
        ZipUtilities.unzipFile(
                exchange.getIn().getBody(InputStream.class),
                bahamutWorkDir + "/adminUnits"
        );
    }

    private void parseStopPlacesNetexFile(Exchange exchange) {
        logger.debug("Parsing the stop place Netex file.");
        var parser = new NetexParser();
        try (Stream<Path> paths = Files.walk(Paths.get(bahamutWorkDir + "/adminUnits"))) {
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

    private void buildAdminUnitCache(Exchange exchange) {
        logger.debug("Building admin units cache.");
        var netexEntitiesIndex = exchange.getIn().getBody(NetexEntitiesIndex.class);
        var adminUnitsCache = AdminUnitsCache.buildNewCache(netexEntitiesIndex, cacheMaxSize);
        exchange.setProperty(ADMIN_UNITS_CACHE_PROPERTY, adminUnitsCache);
    }

    private static void createCurrentFile(Exchange exchange) {
        var outputFileName = exchange.getIn().getHeader(BLOB_STORE_FILE_HANDLE, String.class);
        exchange.getIn().setBody(new ByteArrayInputStream(outputFileName.getBytes()));
        exchange.getIn().setHeader(BLOB_STORE_FILE_HANDLE, "current");
    }

    private static void setOutputFilenameHeaders(Exchange exchange) {
        var outputFilename = "bahamut_export_geocoder_" + System.currentTimeMillis();
        exchange.getIn().setHeader(BLOB_STORE_FILE_HANDLE, outputFilename + ".zip");
        exchange.getIn().setHeader(OUTPUT_FILENAME_HEADER, outputFilename + ".csv");
    }

    private static void logRedelivery(Exchange exchange) {
        var redeliveryCounter = exchange.getIn().getHeader("CamelRedeliveryCounter", Integer.class);
        var redeliveryMaxCounter = exchange.getIn().getHeader("CamelRedeliveryMaxCounter", Integer.class);
        var camelCaughtThrowable = exchange.getProperty("CamelExceptionCaught", Throwable.class);

        logger.warn("Exchange failed, redelivering the message locally, attempt {}/{}...",
                redeliveryCounter, redeliveryMaxCounter, camelCaughtThrowable);
    }

    private static void parseNetexFile(Exchange exchange) {
        logger.debug("Parsing the Netex file.");
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
        logger.debug("Converting netexEntitiesIndex to PeliasDocuments");
        var netexEntitiesIndex = exchange.getIn().getBody(NetexEntitiesIndex.class);
        AdminUnitsCache adminUnitCache = exchange.getProperty(ADMIN_UNITS_CACHE_PROPERTY, AdminUnitsCache.class);
        exchange.getIn().setBody(stopPlacesToPeliasDocument.toPeliasDocuments(netexEntitiesIndex, adminUnitCache));
    }
}