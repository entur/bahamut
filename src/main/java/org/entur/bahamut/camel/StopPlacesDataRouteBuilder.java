package org.entur.bahamut.camel;

import org.apache.camel.Exchange;
import org.entur.bahamut.blobStore.BahamutBlobStoreService;
import org.entur.bahamut.blobStore.KakkaBlobStoreService;
import org.entur.bahamut.peliasDocument.stopPlacestoPeliasDocument.StopPlacesToPeliasDocument;
import org.entur.geocoder.ZipUtilities;
import org.entur.geocoder.camel.ErrorHandlerRouteBuilder;
import org.entur.geocoder.csv.CSVCreator;
import org.entur.geocoder.model.PeliasDocumentList;
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
public class StopPlacesDataRouteBuilder extends ErrorHandlerRouteBuilder {

    private static final Logger logger = LoggerFactory.getLogger(StopPlacesDataRouteBuilder.class);

    public static final String OUTPUT_FILENAME_HEADER = "bahamutOutputFilename";

    @Value("${blobstore.gcs.kakka.tiamat.geocoder.file:tiamat/geocoder/tiamat_export_geocoder_latest.zip}")
    private String tiamatGeocoderFile;

    @Value("${bahamut.workdir:/tmp/bahamut/geocoder}")
    private String bahamutWorkDir;

    private final KakkaBlobStoreService kakkaBlobStoreService;
    private final BahamutBlobStoreService bahamutBlobStoreService;
    private final StopPlacesToPeliasDocument stopPlacesToPeliasDocument;

    // TODO: Do i need camel ???
    public StopPlacesDataRouteBuilder(
            KakkaBlobStoreService kakkaBlobStoreService,
            BahamutBlobStoreService bahamutBlobStoreService,
            StopPlacesToPeliasDocument stopPlacesToPeliasDocument,
            @Value("${bahamut.camel.redelivery.max:3}") int maxRedelivery,
            @Value("${bahamut.camel.redelivery.delay:5000}") int redeliveryDelay,
            @Value("${bahamut.camel.redelivery.backoff.multiplier:3}") int backOffMultiplier) {
        super(maxRedelivery, redeliveryDelay, backOffMultiplier);
        this.kakkaBlobStoreService = kakkaBlobStoreService;
        this.bahamutBlobStoreService = bahamutBlobStoreService;
        this.stopPlacesToPeliasDocument = stopPlacesToPeliasDocument;
    }

    @Override
    public void configure() {

        from("direct:makeCSV")
                .process(this::loadStopPlacesFile)
                .process(this::unzipStopPlacesToWorkingDirectory)
                .process(this::parseStopPlacesNetexFile)
                .process(this::netexEntitiesIndexToPeliasDocument)
                .process(this::createCSVFile)
                .process(this::setOutputFilenameHeader)
                .process(this::zipCSVFile)
                .process(this::uploadCSVFile)
                .process(this::updateCurrentFile);
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

    private void parseStopPlacesNetexFile(Exchange exchange) {
        logger.debug("Parsing the stop place Netex file.");
        var parser = new NetexParser();
        try (Stream<Path> paths = Files.walk(Paths.get(bahamutWorkDir))) {
            paths.filter(StopPlacesDataRouteBuilder::isValidFile).findFirst().ifPresent(path -> {
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
        exchange.getIn().setBody(stopPlacesToPeliasDocument.toPeliasDocuments(netexEntitiesIndex));
    }

    private void createCSVFile(Exchange exchange) {
        logger.debug("Creating CSV file for PeliasDocuments");
        PeliasDocumentList peliasDocuments = exchange.getIn().getBody(PeliasDocumentList.class);
        exchange.getIn().setBody(
                CSVCreator.create(peliasDocuments)
        );
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

    private void updateCurrentFile(Exchange exchange) {
        logger.debug("Updating the current file");
        String currentCSVFileName = exchange.getIn().getHeader(OUTPUT_FILENAME_HEADER, String.class) + ".zip";
        bahamutBlobStoreService.uploadBlob(
                "current",
                new ByteArrayInputStream(currentCSVFileName.getBytes())
        );
    }

    private static boolean isValidFile(Path path) {
        try {
            return Files.isRegularFile(path) && !Files.isHidden(path);
        } catch (IOException e) {
            return false;
        }
    }
}