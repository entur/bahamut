package org.entur.bahamut.camel;

import org.apache.camel.Exchange;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.entur.bahamut.camel.routes.StopPlacesDataRouteBuilder.WORK_DIRECTORY_HEADER;

public final class ZipUtilities {

    public static void unzipFile(Exchange exchange) {
        InputStream inputStream = exchange.getIn().getBody(InputStream.class);
        String targetFolder = exchange.getIn().getHeader(WORK_DIRECTORY_HEADER, String.class);
        unzipFile(inputStream, targetFolder);
    }

    public static void unzipFile(InputStream inputStream, String targetFolder) {
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
            ZipEntry entry = new ZipEntry("tiamat_csv_export_geocoder_latest.csv"); // TODO: hardCoded ???
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
