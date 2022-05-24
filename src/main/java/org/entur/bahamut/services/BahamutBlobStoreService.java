package org.entur.bahamut.services;

import org.entur.bahamut.repository.BlobStoreRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class BahamutBlobStoreService extends BlobStoreService {

    public BahamutBlobStoreService(
            @Value("${blobstore.gcs.bahamut.bucket.name:bahamut-dev}") String bucketName,
            @Autowired BlobStoreRepository repository) {
        super(bucketName, repository);
    }
}