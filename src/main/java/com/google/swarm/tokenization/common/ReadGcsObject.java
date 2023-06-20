package com.google.swarm.tokenization.common;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;

public class ReadGcsObject {
    public static String getGcsObjectContent(String gcsUri){
        String content = null;
        try {
            GcsPath gcsPath = GcsPath.fromUri(gcsUri);
            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobId blobId = BlobId.of(gcsPath.getBucket(), gcsPath.getObject());
            Blob blob = storage.get(blobId);
            content = new String(blob.getContent());
        }catch(Exception e){
            e.printStackTrace();
        }
        return content;
    }
}
