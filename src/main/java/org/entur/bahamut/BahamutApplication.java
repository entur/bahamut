package org.entur.bahamut;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Stream;

@SpringBootApplication
@EnableRetry
public class BahamutApplication implements ApplicationRunner {

    private static final Logger logger = LoggerFactory.getLogger(BahamutApplication.class);

    private final BahamutService bs;

    public BahamutApplication(BahamutService bs) {
        this.bs = bs;
    }

    public static void main(String[] args) {
        SpringApplication.run(BahamutApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) {
        List<InputStream> csvFiles = Stream.of(bs.loadStopPlacesFile())
                .map(bs::unzipStopPlacesToWorkingDirectory)
                .map(bs::parseStopPlacesNetexFile)
                .map(bs::createBahamutData)
                .map(bs::createPeliasDocumentsStream)
                .map(bs::createCSVFile)
                .toList();

        if (!csvFiles.isEmpty()) {
            String outputFilename = bs.getOutputFilename();
            InputStream csvZipFile = bs.zipCSVFile(csvFiles, outputFilename);
            bs.uploadCSVFile(csvZipFile, outputFilename);
            bs.copyCSVFileAsLatestToConfiguredBucket(outputFilename);
            logger.info("Uploaded zipped csv files to bahamut and haya");
        } else {
            logger.info("No csv files generated");
        }
    }
}