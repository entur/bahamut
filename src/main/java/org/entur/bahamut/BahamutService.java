package org.entur.bahamut;

import org.entur.bahamut.blobStore.BahamutBlobStoreService;
import org.entur.bahamut.blobStore.KakkaBlobStoreService;
import org.entur.bahamut.data.BahamutDataBuilder;
import org.entur.bahamut.groupOfStopPlaces.GroupOfStopPlacesPeliasDocumentMapper;
import org.entur.bahamut.data.BahamutData;
import org.entur.bahamut.stopPlaces.StopPlacePeliasDocumentMapper;
import org.entur.geocoder.Utilities;
import org.entur.geocoder.ZipUtilities;
import org.entur.geocoder.csv.CSVCreator;
import org.entur.geocoder.model.PeliasDocument;
import org.entur.netex.NetexParser;
import org.entur.netex.index.api.NetexEntitiesIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

@Service
public class BahamutService {

    private static final Logger logger = LoggerFactory.getLogger(BahamutService.class);

    @Value("${blobstore.gcs.kakka.tiamat.geocoder.file:tiamat/geocoder/tiamat_export_geocoder_latest.zip}")
    private String tiamatGeocoderFile;

    @Value("${bahamut.workdir:/tmp/bahamut/geocoder}")
    private String bahamutWorkDir;

    private final boolean gosInclude;
    private final BahamutDataBuilder bahamutDataBuilder;
    private final KakkaBlobStoreService kakkaBlobStoreService;
    private final BahamutBlobStoreService bahamutBlobStoreService;
    private final StopPlacePeliasDocumentMapper stopPlacesToPeliasDocument;
    private final GroupOfStopPlacesPeliasDocumentMapper groupOfStopPlacesPeliasDocument;

    public BahamutService(
            BahamutDataBuilder bahamutDataBuilder,
            KakkaBlobStoreService kakkaBlobStoreService,
            BahamutBlobStoreService bahamutBlobStoreService,
            StopPlacePeliasDocumentMapper stopPlacesToPeliasDocument,
            GroupOfStopPlacesPeliasDocumentMapper groupOfStopPlacesPeliasDocument,
            @Value("${bahamut.gos.include:true}") boolean gosInclude) {
        this.bahamutDataBuilder = bahamutDataBuilder;
        this.kakkaBlobStoreService = kakkaBlobStoreService;
        this.bahamutBlobStoreService = bahamutBlobStoreService;
        this.stopPlacesToPeliasDocument = stopPlacesToPeliasDocument;
        this.groupOfStopPlacesPeliasDocument = groupOfStopPlacesPeliasDocument;
        this.gosInclude = gosInclude;
    }

    @Retryable(
            value = Exception.class,
            maxAttemptsExpression = "${bahamut.retry.maxAttempts:3}",
            backoff = @Backoff(
                    delayExpression = "${bahamut.retry.maxDelay:5000}",
                    multiplierExpression = "${bahamut.retry.backoff.multiplier:3}"))
    public InputStream loadStopPlacesFile() {
        logger.info("Loading stop places file");
        return kakkaBlobStoreService.getBlob(tiamatGeocoderFile);
    }

    public Path unzipStopPlacesToWorkingDirectory(InputStream inputStream) {
        logger.info("Unzipping stop places file");
        ZipUtilities.unzipFile(inputStream, bahamutWorkDir);
        try (Stream<Path> paths = Files.walk(Paths.get(bahamutWorkDir))) {
            return paths
                    .filter(Utilities::isValidFile)
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException("Unzipped file not found."));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public NetexEntitiesIndex parseStopPlacesNetexFile(Path path) {
        logger.info("Parsing the stop place Netex file");
        var parser = new NetexParser();
        try (InputStream inputStream = new FileInputStream(path.toFile())) {
            return parser.parse(inputStream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public BahamutData createBahamutData(NetexEntitiesIndex netexEntitiesIndex) {
        logger.info("Creating bahamut data object");
        return bahamutDataBuilder.build(netexEntitiesIndex);
    }

    public Stream<PeliasDocument> createPeliasDocumentsStream(BahamutData bahamutData) {
        logger.info("Creating PeliasDocuments stream");
        Stream<PeliasDocument> stopPlacesStream =
                stopPlacesToPeliasDocument.toPeliasDocuments(bahamutData);

        if (gosInclude) {
            Stream<PeliasDocument> groupOfStopPlacesStream =
                    groupOfStopPlacesPeliasDocument.toPeliasDocuments(bahamutData);
            return Stream.concat(stopPlacesStream, groupOfStopPlacesStream);
        } else {
            return stopPlacesStream;
        }
    }

    public InputStream createCSVFile(Stream<PeliasDocument> peliasDocuments) {
        logger.info("Creating CSV file form PeliasDocuments stream");
        return CSVCreator.create(peliasDocuments);
    }

    public String getOutputFilename() {
        return "bahamut_export_geocoder_" + System.currentTimeMillis();
    }

    public InputStream zipCSVFile(List<InputStream> inputStreams, String filename) {
        logger.info("Zipping the created csv file");
        return ZipUtilities.zipFiles(inputStreams, filename + ".csv");
    }

    @Retryable(
            value = Exception.class,
            maxAttemptsExpression = "${bahamut.retry.maxAttempts:3}",
            backoff = @Backoff(
                    delayExpression = "${bahamut.retry.maxDelay:5000}",
                    multiplierExpression = "${bahamut.retry.backoff.multiplier:3}"))
    public void uploadCSVFile(InputStream csvZipFile, String filename) {
        logger.info("Uploading the zipped CSV file top bahamut");
        bahamutBlobStoreService.uploadBlob(filename + ".zip", csvZipFile);
    }

    @Retryable(
            value = Exception.class,
            maxAttemptsExpression = "${bahamut.retry.maxAttempts:3}",
            backoff = @Backoff(
                    delayExpression = "${bahamut.retry.maxDelay:5000}",
                    multiplierExpression = "${bahamut.retry.backoff.multiplier:3}"))
    public void copyCSVFileAsLatestToConfiguredBucket(String filename) {
        logger.info("Coping latest file to haya");
        bahamutBlobStoreService.copyBlobAsLatestToTargetBucket(filename + ".zip");
    }
}