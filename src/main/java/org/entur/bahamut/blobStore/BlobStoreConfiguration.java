package org.entur.bahamut.blobStore;

import com.google.cloud.storage.Storage;
import org.entur.geocoder.blobStore.GcsBlobStoreRepository;
import org.entur.geocoder.blobStore.InMemoryBlobStoreRepository;
import org.entur.geocoder.blobStore.LocalDiskBlobStoreRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class BlobStoreConfiguration {

    @Bean
    @Scope("prototype")
    @Profile("gcs-blobstore")
    public GcsBlobStoreRepository getGcsBlobStoreRepository(Storage storage) {
        return new GcsBlobStoreRepository(storage);
    }

    @Bean
    @Scope("prototype")
    @Profile("local-disk-blobstore")
    public LocalDiskBlobStoreRepository getLocalDiskBlobStoreRepository(
            @Value("${blobstore.local.folder:files/blob}") String baseFolder) {
        return new LocalDiskBlobStoreRepository(baseFolder);
    }

    @Bean
    @Scope("prototype")
    @Profile("in-memory-blobstore")
    public InMemoryBlobStoreRepository getInMemoryBlobStoreRepository(
            Map<String, Map<String, byte[]>> blobsInContainers) {
        return new InMemoryBlobStoreRepository(blobsInContainers);
    }

    @Bean
    @Profile("in-memory-blobstore")
    public Map<String, Map<String, byte[]>> blobsInContainers() {
        return Collections.synchronizedMap(new HashMap<>());
    }
}
