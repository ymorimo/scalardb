package com.scalar.db.storage.blob;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import javax.annotation.Nullable;

public class BlobUtils {
  public static final String OBJECT_KEY_DELIMITER = "/";
  public static final String PARTITION_KEY_DELIMITER = "*";

  public static BlobContainerClient buildBlobContainerClient(BlobConfig config) {
    return new BlobServiceClientBuilder()
        .endpoint(config.getEndpoint())
        .credential(new StorageSharedKeyCredential(config.getAccountName(), config.getAccountKey()))
        .buildClient()
        .getBlobContainerClient(config.getContainer());
  }

  public static String getObjectKey(
      String namespace,
      String table,
      @Nullable String partition,
      @Nullable String concatenatedKey) {
    if (partition == null) {
      if (concatenatedKey == null) {
        return String.join(OBJECT_KEY_DELIMITER, namespace, table, "");
      } else {
        return String.join(OBJECT_KEY_DELIMITER, namespace, table, concatenatedKey);
      }
    } else {
      if (concatenatedKey == null) {
        return String.join(OBJECT_KEY_DELIMITER, namespace, table, partition, "");
      } else {
        return String.join(OBJECT_KEY_DELIMITER, namespace, table, partition, concatenatedKey);
      }
    }
  }
}
