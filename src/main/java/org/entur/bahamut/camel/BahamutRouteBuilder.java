package org.entur.bahamut.camel;

import org.apache.camel.Exchange;
import org.entur.bahamut.blobStore.BahamutBlobStoreService;
import org.entur.bahamut.blobStore.KakkaBlobStoreService;
import org.entur.bahamut.groupOfStopPlaces.GroupOfStopPlacePeliasDocumentMapper;
import org.entur.bahamut.BahamutData;
import org.entur.bahamut.stopPlaces.StopPlacePeliasDocumentMapper;
import org.entur.bahamut.stopPlaces.stopPlacePopularityCache.StopPlacesPopularityCache;
import org.entur.bahamut.stopPlaces.stopPlacePopularityCache.StopPlacesPopularityCacheBuilder;
import org.entur.geocoder.Utilities;
import org.entur.geocoder.ZipUtilities;
import org.entur.geocoder.camel.ErrorHandlerRouteBuilder;
import org.entur.geocoder.csv.CSVCreator;
import org.entur.geocoder.model.PeliasDocument;
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
import java.util.stream.Stream;

@Component
public class BahamutRouteBuilder extends ErrorHandlerRouteBuilder {

    private static final Logger logger = LoggerFactory.getLogger(BahamutRouteBuilder.class);

    public static final String OUTPUT_FILENAME_HEADER = "bahamutOutputFilename";
    private static final String STOP_PLACES_POPULARITY_CACHE_PROPERTY = "StopPlacesPopularity";

    @Value("${blobstore.gcs.kakka.tiamat.geocoder.file:tiamat/geocoder/tiamat_export_geocoder_latest.zip}")
    private String tiamatGeocoderFile;

    @Value("${bahamut.workdir:/tmp/bahamut/geocoder}")
    private String bahamutWorkDir;

    private final KakkaBlobStoreService kakkaBlobStoreService;
    private final BahamutBlobStoreService bahamutBlobStoreService;
    private final StopPlacesPopularityCacheBuilder stopPlacesPopularityCacheBuilder;
    private final StopPlacePeliasDocumentMapper stopPlacesToPeliasDocument;
    private final GroupOfStopPlacePeliasDocumentMapper groupOfStopPlacePeliasDocument;
    private final boolean gosInclude;

    // TODO: Do i need camel ??? What about retries if i remove camel ? spring-retry ??
    public BahamutRouteBuilder(
            KakkaBlobStoreService kakkaBlobStoreService,
            BahamutBlobStoreService bahamutBlobStoreService,
            StopPlacePeliasDocumentMapper stopPlacesToPeliasDocument,
            GroupOfStopPlacePeliasDocumentMapper groupOfStopPlacePeliasDocument,
            StopPlacesPopularityCacheBuilder stopPlacesPopularityCacheBuilder,
            @Value("${bahamut.gos.include:true}") boolean gosInclude,
            @Value("${bahamut.camel.redelivery.max:3}") int maxRedelivery,
            @Value("${bahamut.camel.redelivery.delay:5000}") int redeliveryDelay,
            @Value("${bahamut.camel.redelivery.backoff.multiplier:3}") int backOffMultiplier) {
        super(maxRedelivery, redeliveryDelay, backOffMultiplier);
        this.kakkaBlobStoreService = kakkaBlobStoreService;
        this.bahamutBlobStoreService = bahamutBlobStoreService;
        this.stopPlacesToPeliasDocument = stopPlacesToPeliasDocument;
        this.groupOfStopPlacePeliasDocument = groupOfStopPlacePeliasDocument;
        this.gosInclude = gosInclude;
        this.stopPlacesPopularityCacheBuilder = stopPlacesPopularityCacheBuilder;
    }

    @Override
    public void configure() {

        from("direct:makeCSV")
                .process(this::loadStopPlacesFile)
                .process(this::unzipStopPlacesToWorkingDirectory)
                .process(this::parseStopPlacesNetexFile)
                .process(this::createBahamutData)
                .process(this::fillStopPlacesPopularityCache)
                .process(this::createPeliasDocumentsStream)
                .process(this::createCSVFile)
                .process(this::setOutputFilenameHeader)
                .process(this::zipCSVFile)
                .process(this::uploadCSVFile)
                .process(this::copyCSVFileAsLatestToConfiguredBucket);
    }

    private void runProcess(Exchange exchange) {
        loadStopPlacesFile(exchange);
    }

    private void loadStopPlacesFile(Exchange exchange) {
        logger.debug("Loading stop places file");
        exchange.getIn().setBody(
                kakkaBlobStoreService.getBlob(tiamatGeocoderFile),
                InputStream.class
        );
    }

    private void unzipStopPlacesToWorkingDirectory(Exchange exchange) {
        logger.debug("Unzipping stop places file");
        ZipUtilities.unzipFile(
                exchange.getIn().getBody(InputStream.class),
                bahamutWorkDir
        );
    }

    private void parseStopPlacesNetexFile(Exchange exchange) throws IOException {
        logger.debug("Parsing the stop place Netex file.");
        var parser = new NetexParser();
        try (Stream<Path> paths = Files.walk(Paths.get(bahamutWorkDir))) {
            paths.filter(Utilities::isValidFile).findFirst().ifPresent(path -> {
                try (InputStream inputStream = new FileInputStream(path.toFile())) {
                    exchange.getIn().setBody(parser.parse(inputStream));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void createBahamutData(Exchange exchange) {
        logger.debug("Creating bahamut data object");
        var netexEntitiesIndex = exchange.getIn().getBody(NetexEntitiesIndex.class);
        exchange.getIn().setBody(BahamutData.create(netexEntitiesIndex));
    }

    private void fillStopPlacesPopularityCache(Exchange exchange) {
        logger.debug("Calculating and caching stop places popularity");
        BahamutData bahamutData = exchange.getIn().getBody(BahamutData.class);
        exchange.setProperty(
                STOP_PLACES_POPULARITY_CACHE_PROPERTY,
                stopPlacesPopularityCacheBuilder.build(bahamutData.stopPlaceHierarchies()));
    }

    private void createPeliasDocumentsStream(Exchange exchange) {
        logger.debug("Creating PeliasDocuments stream");
        var bahamutData = exchange.getIn().getBody(BahamutData.class);
        var stopPlacePopularityCache =
                exchange.getProperty(STOP_PLACES_POPULARITY_CACHE_PROPERTY, StopPlacesPopularityCache.class);
        Stream<PeliasDocument> stopPlacesStream = stopPlacesToPeliasDocument.toPeliasDocuments(
                bahamutData.stopPlaceHierarchies(),
                stopPlacePopularityCache);
        if (gosInclude) {
            Stream<PeliasDocument> groupOfStopPlacesStream = groupOfStopPlacePeliasDocument.toPeliasDocuments(
                    bahamutData.groupOfStopPlaces(),
                    stopPlacePopularityCache);
            exchange.getIn().setBody(Stream.concat(stopPlacesStream, groupOfStopPlacesStream));
        } else {
            exchange.getIn().setBody(stopPlacesStream);
        }
    }

    private void createCSVFile(Exchange exchange) {
        logger.debug("Creating CSV file form PeliasDocuments stream");
        @SuppressWarnings("unchecked")
        Stream<PeliasDocument> peliasDocuments = exchange.getIn().getBody(Stream.class);
        exchange.getIn().setBody(CSVCreator.create(peliasDocuments));
    }

    private void setOutputFilenameHeader(Exchange exchange) {
        exchange.getIn().setHeader(
                OUTPUT_FILENAME_HEADER,
                "bahamut_export_geocoder_" + System.currentTimeMillis()
        );
    }

    private void zipCSVFile(Exchange exchange) {
        logger.debug("Zipping the created csv file");
        ByteArrayInputStream zipFile = ZipUtilities.zipFile(
                exchange.getIn().getBody(InputStream.class),
                exchange.getIn().getHeader(OUTPUT_FILENAME_HEADER, String.class) + ".csv"
        );
        exchange.getIn().setBody(zipFile);
    }

    private void uploadCSVFile(Exchange exchange) {
        logger.debug("Uploading the CSV file");
        bahamutBlobStoreService.uploadBlob(
                exchange.getIn().getHeader(OUTPUT_FILENAME_HEADER, String.class) + ".zip",
                exchange.getIn().getBody(InputStream.class)
        );
    }

    private void copyCSVFileAsLatestToConfiguredBucket(Exchange exchange) {
        logger.debug("Coping latest file to haya");
        String currentCSVFileName = exchange.getIn().getHeader(OUTPUT_FILENAME_HEADER, String.class) + ".zip";
        bahamutBlobStoreService.copyBlobAsLatestToTargetBucket(currentCSVFileName);
    }
}