package com.scalar.db.storage.objectstorage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import javax.annotation.Nullable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

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
    if (Objects.equals(config.getStorageType(), S3Wrapper.STORAGE_TYPE)) {
      return new S3Wrapper(buildS3Client(config), config.getBucket());
    } else if (Objects.equals(config.getStorageType(), BlobStorageWrapper.STORAGE_TYPE)) {
      return new BlobStorageWrapper(buildBlobContainerClient(config), config);
    } else if (Objects.equals(config.getStorageType(), CloudStorageWrapper.STORAGE_TYPE)) {
      return new CloudStorageWrapper(buildStorage(config), config.getBucket());
    } else {
      throw new IllegalArgumentException("Unsupported storage type: " + config.getStorageType());
    }
  }

  private static S3Client buildS3Client(ObjectStorageConfig config) {
    S3ClientBuilder builder = S3Client.builder();
    config.getEndpointOverride().ifPresent(e -> builder.endpointOverride(URI.create(e)));
    return builder
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(config.getUsername(), config.getPassword())))
        .region(Region.of(config.getRegion()))
        .forcePathStyle(true)
        .build();
  }

  private static BlobContainerClient buildBlobContainerClient(ObjectStorageConfig config) {
    return new BlobServiceClientBuilder()
        .endpoint(config.getEndpoint())
        .credential(new StorageSharedKeyCredential(config.getUsername(), config.getPassword()))
        .buildClient()
        .getBlobContainerClient(config.getBucket());
  }

  private static Storage buildStorage(ObjectStorageConfig config) {
    try {
      return StorageOptions.newBuilder()
          .setProjectId(config.getProjectId())
          .setCredentials(
              ServiceAccountCredentials.fromStream(
                  Files.newInputStream(Paths.get(config.getPassword()))))
          .build()
          .getService();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Cloud Storage client", e);
    }
  }
}
