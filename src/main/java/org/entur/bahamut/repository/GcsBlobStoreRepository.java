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

package org.entur.bahamut.repository;

import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import org.entur.bahamut.camel.adminUnitsRepository.BlobStoreFiles;
import org.rutebanken.helper.gcp.BlobStoreHelper;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Repository;

import java.io.InputStream;
import java.util.*;

/**
 * Blob store no.entur.antu.repository targeting Google Cloud Storage.
 */
@Repository
@Profile("gcs-blobstore")
@Scope("prototype")
public class GcsBlobStoreRepository implements BlobStoreRepository {

    private Storage storage;

    private String bucketName;

    public GcsBlobStoreRepository(Storage storage) {
        this.storage = storage;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    @Override
    public boolean existBlob(String objectName) {
        return BlobStoreHelper.existBlob(storage, bucketName, objectName);
    }

    @Override
    public InputStream getBlob(String name) {
        return BlobStoreHelper.getBlob(storage, bucketName, name);
    }

    @Override
    public void uploadBlob(String name, InputStream inputStream) {
        BlobStoreHelper.uploadBlobWithRetry(storage, bucketName, name, inputStream, false);
    }

    @Override
    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    @Override
    public BlobStoreFiles listBlobs(String prefix) {
        return listBlobs(List.of(prefix));
    }

    private BlobStoreFiles listBlobs(Collection<String> prefixes) {
        BlobStoreFiles blobStoreFiles = new BlobStoreFiles();


        for (String prefix : prefixes) {
            Iterator<Blob> blobIterator = BlobStoreHelper.listAllBlobsRecursively(storage, bucketName, prefix);
            blobIterator.forEachRemaining(blob -> blobStoreFiles.add(toBlobStoreFile(blob, blob.getName())));
        }

        return blobStoreFiles;
    }

    private BlobStoreFiles.File toBlobStoreFile(Blob blob, String fileName) {
        BlobStoreFiles.File file = new BlobStoreFiles.File(fileName, new Date(blob.getCreateTime()), new Date(blob.getUpdateTime()), blob.getSize());

        if (blob.getAcl() != null) {
            if (blob.getAcl().stream().anyMatch(acl -> Acl.User.ofAllUsers().equals(acl.getEntity()) && acl.getRole() != null)) {
                file.setUrl(blob.getMediaLink());
            }
        }
        return file;
    }
}
