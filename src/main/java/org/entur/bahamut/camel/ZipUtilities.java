package org.entur.bahamut.camel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public final class ZipUtilities {

    private static final Logger logger = LoggerFactory.getLogger(ZipUtilities.class);

    public static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
        File destFile = new File(destinationDir, zipEntry.getName());

        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();

        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destFile;
    }

    public static void unzipFile(InputStream inputStream, String targetFolder) {
        var buffer = new byte[1024];
        var destDir = new File(targetFolder);
        try (var zis = new ZipInputStream(inputStream)) {
            var zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                var newFile = newFile(destDir, zipEntry);
                if (zipEntry.isDirectory()) {
                    if (!newFile.isDirectory() && !newFile.mkdirs()) {
                        throw new IOException("Failed to create directory " + newFile);
                    }
                } else {
                    // This is necessary for archives created on Windows,
                    // where the root directories don't have a corresponding entry in the zip file.
                    // https://www.baeldung.com/java-compress-and-uncompress
                    var parent = newFile.getParentFile();
                    if (!parent.isDirectory() && !parent.mkdirs()) {
                        throw new IOException("Failed to create directory " + parent);
                    }

                    // write file content
                    var fos = new FileOutputStream(newFile);
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        fos.write(buffer, 0, len);
                    }
                    fos.close();
                }
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
        } catch (IOException ioE) {
            throw new RuntimeException("Unzipping archive failed: " + ioE.getMessage(), ioE);
        }
    }

    public static ByteArrayInputStream zipFile(InputStream inputStream, String outputFilename) {
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
            return new ByteArrayInputStream(baos.toByteArray());
        } catch (Exception ex) {
            throw new RuntimeException("Failed to add file to zip: " + ex.getMessage(), ex);
        }
    }
}
