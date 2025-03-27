package com.scalar.db.storage.objectstorage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.Objects;
import javax.annotation.Nullable;

public class ObjectStorageUtils {
  public static final String OBJECT_KEY_DELIMITER = "/";
  public static final String CONCATENATED_KEY_DELIMITER = "*";

  public static String getObjectKey(String namespace, String table, @Nullable String partition) {
    if (partition == null) {
      return String.join(OBJECT_KEY_DELIMITER, namespace, table);
    } else {
      return String.join(OBJECT_KEY_DELIMITER, namespace, table, partition);
    }
  }

  public static ObjectStorageWrapper getObjectStorageWrapper(ObjectStorageConfig config) {
    if (Objects.equals(config.getStorageType(), BlobStorageWrapper.STORAGE_TYPE)) {
      return new BlobStorageWrapper(buildBlobContainerClient(config));
    } else {
      throw new IllegalArgumentException("Unsupported storage type: " + config.getStorageType());
    }
  }

  private static BlobContainerClient buildBlobContainerClient(ObjectStorageConfig config) {
    return new BlobServiceClientBuilder()
        .endpoint(config.getEndpoint())
        .credential(new StorageSharedKeyCredential(config.getUsername(), config.getPassword()))
        .buildClient()
        .getBlobContainerClient(config.getBucket());
  }
}
