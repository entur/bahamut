package org.entur.bahamut.camel;

import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.entur.bahamut.camel.routes.StopPlacesDataRouteBuilder.OUTPUT_FILENAME_HEADER;
import static org.entur.bahamut.camel.routes.StopPlacesDataRouteBuilder.WORK_DIRECTORY_HEADER;

public final class ZipUtilities {

    private static final Logger logger = LoggerFactory.getLogger(ZipUtilities.class);

    public static void unzipFile(Exchange exchange) {
        var inputStream = exchange.getIn().getBody(InputStream.class);
        var targetFolder = exchange.getIn().getHeader(WORK_DIRECTORY_HEADER, String.class);
        unzipFile(inputStream, targetFolder);
    }

    public static void unzipFile(InputStream inputStream, String targetFolder) {
        try (ZipInputStream zis = new ZipInputStream(inputStream)) {
            var buffer = new byte[1024];
            var zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                var fileName = zipEntry.getName();
                logger.info("unzipping file {} in folder {} ", fileName, targetFolder);

                var path = Path.of(targetFolder + "/" + fileName);
                if (Files.isDirectory(path)) {
                    path.toFile().mkdirs();
                    continue;
                }

                File parent = path.toFile().getParentFile();
                if (parent != null) {
                    parent.mkdirs();
                }


                var fos = new FileOutputStream(path.toFile());
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
        var inputStream = exchange.getIn().getBody(InputStream.class);
        var outputFilename = exchange.getIn().getHeader(OUTPUT_FILENAME_HEADER, String.class);
        logger.info("zipping file {}", outputFilename);
        try {
            var inputBytes = inputStream.readAllBytes();
            var baos = new ByteArrayOutputStream();
            var zos = new ZipOutputStream(baos);
            var entry = new ZipEntry(outputFilename);
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
}
