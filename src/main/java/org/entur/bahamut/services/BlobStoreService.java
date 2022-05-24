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

package org.entur.bahamut.services;

import org.apache.camel.Header;
import org.entur.bahamut.repository.BlobStoreRepository;

import java.io.InputStream;

public abstract class BlobStoreService {
    public static final String FILE_HANDLE = "EnturFileHandle";

    protected final BlobStoreRepository repository;

    protected BlobStoreService(String bucketName, BlobStoreRepository repository) {
        this.repository = repository;
        this.repository.setBucketName(bucketName);
        System.out.println();
    }

    public boolean existBlob(@Header(value = FILE_HANDLE) String name) {
        return repository.existBlob(name);
    }

    public InputStream getBlob(@Header(value = FILE_HANDLE) String name) {
        return repository.getBlob(name);
    }

    public void uploadBlob(@Header(value = FILE_HANDLE) String name, InputStream inputStream) {
        repository.uploadBlob(name, inputStream);
    }
}