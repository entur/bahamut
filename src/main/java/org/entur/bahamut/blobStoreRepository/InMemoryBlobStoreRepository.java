/*
 *
 *  * Licensed under the EUPL, Version 1.2 or – as soon they will be approved by
 *  * the European Commission - subsequent versions of the EUPL (the "Licence");
 *  * You may not use this work except in compliance with the Licence.
 *  * You may obtain a copy of the Licence at:
 *  *
 *  *   https://joinup.ec.europa.eu/software/page/eupl
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the Licence is distributed on an "AS IS" basis,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the Licence for the specific language governing permissions and
 *  * limitations under the Licence.
 *  *
 *
 */

package org.entur.bahamut.blobStoreRepository;

import com.google.cloud.storage.Storage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Simple memory-based blob store no.entur.antu.repository for testing purpose.
 */
@Repository
@Profile("in-memory-blobstore")
@Scope("prototype")
public class InMemoryBlobStoreRepository implements BlobStoreRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryBlobStoreRepository.class);

    private final Map<String, Map<String, byte[]>> blobsInContainers;

    private String containerName;

    public InMemoryBlobStoreRepository(Map<String, Map<String, byte[]>> blobsInContainers) {
        this.blobsInContainers = blobsInContainers;
    }

    private Map<String, byte[]> getBlobsForCurrentContainer() {
        return getBlobsForContainer(containerName);
    }

    private Map<String, byte[]> getBlobsForContainer(String aContainer) {
        return blobsInContainers.computeIfAbsent(aContainer, k -> Collections.synchronizedMap(new HashMap<>()));
    }

    @Override
    public boolean existBlob(String objectName) {
        LOGGER.debug("existBlob called in in-memory blob store");
        byte[] data = getBlobsForCurrentContainer().get(objectName);
        return data != null;
    }

    @Override
    public InputStream getBlob(String objectName) {
        LOGGER.debug("get blob called in in-memory blob store");
        byte[] data = getBlobsForCurrentContainer().get(objectName);
        if (data != null) {
            return new ByteArrayInputStream(data);
        } else {
            LOGGER.info("File '{}' in bucket '{}' does not exist", objectName, containerName);
            return null;
        }
    }

    @Override
    public void uploadBlob(String objectName, InputStream inputStream) {
        try {
            LOGGER.debug("upload blob called in in-memory blob store");
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            IOUtils.copy(inputStream, byteArrayOutputStream);
            byte[] data = byteArrayOutputStream.toByteArray();
            if (data.length == 0) {
                LOGGER.warn("The uploaded file {} is empty", objectName);
            }
            getBlobsForCurrentContainer().put(objectName, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setBucketName(String bucketName) {
        this.containerName = bucketName;
    }

    @Override
    public void setStorage(Storage storage) {
        // TODO: Not good
    }

    @Override
    public BlobStoreFiles listBlobs(String prefix) {
        return listBlobs(Arrays.asList(prefix));
    }

    private BlobStoreFiles listBlobs(Collection<String> prefixes) {
        // TODO: logger.debug("list blobs called in in-memory blob store");
        List<BlobStoreFiles.File> files = blobsInContainers.keySet().stream()
                .filter(k -> prefixes.stream().anyMatch(prefix -> k.startsWith(prefix)))
                .map(k -> new BlobStoreFiles.File(k, new Date(), new Date(), 1234L))    //TODO Add real details?
                .collect(Collectors.toList());
        BlobStoreFiles blobStoreFiles = new BlobStoreFiles();
        blobStoreFiles.add(files);
        return blobStoreFiles;
    }
}
