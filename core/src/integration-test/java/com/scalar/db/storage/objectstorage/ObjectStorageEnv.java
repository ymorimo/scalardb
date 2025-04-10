package com.scalar.db.storage.objectstorage;

import com.scalar.db.config.DatabaseConfig;
import java.util.Objects;
import java.util.Properties;

public class ObjectStorageEnv {
  private static final String PROP_OBJECT_STORAGE_ENDPOINT = "scalardb.object_storage.endpoint";
  private static final String PROP_OBJECT_STORAGE_USERNAME = "scalardb.object_storage.username";
  private static final String PROP_OBJECT_STORAGE_PASSWORD = "scalardb.object_storage.password";
  private static final String PROP_OBJECT_STORAGE_STORAGE_TYPE =
      "scalardb.object_storage.storage_type";
  private static final String PROP_OBJECT_STORAGE_BUCKET = "scalardb.object_storage.bucket";
  // For S3
  private static final String PROP_OBJECT_STORAGE_REGION = "scalardb.s3.region";
  private static final String PROP_S3_ENDPOINT_OVERRIDE = "scalardb.s3.endpoint_override";
  // For Cloud Storage
  private static final String PROP_OBJECT_STORAGE_PROJECT_ID = "scalardb.gcs.project_id";

  private static final String DEFAULT_BLOB_ENDPOINT = "http://localhost:10000/";
  private static final String DEFAULT_BLOB_USERNAME = "devstoreaccount1";
  private static final String DEFAULT_BLOB_PASSWORD =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
  private static final String DEFAULT_BLOB_CONTAINER = "fake-container";
  // For S3
  private static final String DEFAULT_BLOB_REGION = "us-west-2";
  // For Cloud Storage
  private static final String DEFAULT_GCS_PROJECT_ID = "test-project";

  private ObjectStorageEnv() {}

  public static Properties getProperties(String testName) {
    String accountName = System.getProperty(PROP_OBJECT_STORAGE_USERNAME, DEFAULT_BLOB_USERNAME);
    String accountKey = System.getProperty(PROP_OBJECT_STORAGE_PASSWORD, DEFAULT_BLOB_PASSWORD);
    String endpoint =
        System.getProperty(PROP_OBJECT_STORAGE_ENDPOINT, DEFAULT_BLOB_ENDPOINT) + accountName;
    String storage_type =
        System.getProperty(PROP_OBJECT_STORAGE_STORAGE_TYPE, BlobStorageWrapper.STORAGE_TYPE);
    String bucket = System.getProperty(PROP_OBJECT_STORAGE_BUCKET, DEFAULT_BLOB_CONTAINER);

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, endpoint);
    properties.setProperty(DatabaseConfig.USERNAME, accountName);
    properties.setProperty(DatabaseConfig.PASSWORD, accountKey);
    properties.setProperty(DatabaseConfig.STORAGE, ObjectStorageConfig.STORAGE_NAME);
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_FILTERING, "true");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN_ORDERING, "false");
    properties.setProperty(ObjectStorageConfig.STORAGE_TYPE, storage_type);
    properties.setProperty(ObjectStorageConfig.BUCKET, bucket);

    // For S3
    if (Objects.equals(storage_type, S3Wrapper.STORAGE_TYPE)) {
      String region = System.getProperty(PROP_OBJECT_STORAGE_REGION, DEFAULT_BLOB_REGION);
      String endpointOverride = System.getProperty(PROP_S3_ENDPOINT_OVERRIDE, null);
      if (endpointOverride != null) {
        properties.setProperty(ObjectStorageConfig.ENDPOINT_OVERRIDE, endpointOverride);
      }
      properties.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    }

    // For Cloud Storage
    if (Objects.equals(storage_type, CloudStorageWrapper.STORAGE_TYPE)) {
      String projectId = System.getProperty(PROP_OBJECT_STORAGE_PROJECT_ID, DEFAULT_GCS_PROJECT_ID);
      properties.setProperty(ObjectStorageConfig.PROJECT_ID, projectId);
    }

    // Add testName as a metadata namespace suffix
    properties.setProperty(
        DatabaseConfig.SYSTEM_NAMESPACE_NAME,
        DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME + "_" + testName);

    return properties;
  }
}
