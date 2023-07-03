package com.google.swarm.tokenization.common;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ReadGcsObject {
    public static final Logger LOG = LoggerFactory.getLogger(ReadGcsObject.class);
    public static String getGcsObjectContent(String gcsUri){
        String content = null;
        try {
            GcsPath gcsPath = GcsPath.fromUri(gcsUri);
            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobId blobId = BlobId.of(gcsPath.getBucket(), gcsPath.getObject());
            Blob blob = storage.get(blobId);
            content = new String(blob.getContent());
        }catch(Exception e){
            LOG.error("Failed to read GCS content object from "+gcsUri+" due to :"+e.getMessage());
            throw new RuntimeException("Failed to read GCS content object from "+gcsUri+" due to :"+e.getMessage());
        }
        return content;
    }
}
