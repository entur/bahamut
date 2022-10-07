package org.entur.bahamut.blobStore;

import org.entur.geocoder.blobStore.BlobStoreRepository;
import org.entur.geocoder.blobStore.BlobStoreService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class BahamutBlobStoreService extends BlobStoreService {

    @Value("${blobstore.gcs.haya.bucket.name:haya-dev}")
    private String targetBucketName;

    @Value("${blobstore.gcs.latest.filename_without_extension:bahamut_latest}")
    private String targetFilename;

    public BahamutBlobStoreService(
            @Value("${blobstore.gcs.bahamut.bucket.name:bahamut-dev}") String bucketName,
            @Autowired BlobStoreRepository repository) {
        super(bucketName, repository);
    }

    public void copyBlobAsLatestToTargetBucket(String sourceName) {
        super.copyBlob(sourceName, targetBucketName, targetFilename + ".zip");
    }
}