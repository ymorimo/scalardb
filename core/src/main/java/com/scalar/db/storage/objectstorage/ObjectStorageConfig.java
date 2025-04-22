package com.scalar.db.storage.objectstorage;

import static com.scalar.db.config.ConfigUtils.getInt;
import static com.scalar.db.config.ConfigUtils.getLong;
import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStorageConfig {
  public static final String STORAGE_NAME = "object_storage";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  public static final String STORAGE_TYPE = PREFIX + "type";
  public static final String BUCKET = PREFIX + "bucket";

  // For S3
  public static final String ENDPOINT_OVERRIDE = PREFIX + "endpoint_override";

  // For Blob Storage
  public static final String BLOB_BLOCK_SIZE =
      PREFIX + BlobStorageWrapper.STORAGE_TYPE + ".block_size";
  public static final String BLOB_MAX_UPLOAD_CONCURRENCY =
      PREFIX + BlobStorageWrapper.STORAGE_TYPE + ".max_upload_concurrency";
  public static final String BLOB_MAX_SINGLE_UPLOAD_SIZE =
      PREFIX + BlobStorageWrapper.STORAGE_TYPE + ".max_single_upload_size";
  public static final String BLOB_TIMEOUT_IN_SECONDS =
      PREFIX + BlobStorageWrapper.STORAGE_TYPE + ".timeout_in_seconds";

  // For Cloud Storage
  public static final String PROJECT_ID = PREFIX + "project_id";

  /**
   * @deprecated As of 5.0, will be removed.
   */
  @Deprecated
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";

  private static final Logger logger = LoggerFactory.getLogger(ObjectStorageConfig.class);
  private final String endpoint;
  private final String username;
  private final String password;
  private final String storage_type;
  private final String bucket;
  private final String metadataNamespace;

  // For S3
  private final String region;
  private final String endpointOverride;

  // For Blob Storage
  private final long blockSize;
  private final int maxUploadConcurrency;
  private final long maxSingleUploadSize;
  private final int timeoutInSeconds;

  // For Cloud Storage
  private final String projectId;

  public ObjectStorageConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(CoreError.INVALID_CONTACT_POINTS.buildMessage());
    }
    endpoint = databaseConfig.getContactPoints().get(0);
    username = databaseConfig.getUsername().orElse(null);
    password = databaseConfig.getPassword().orElse(null);
    if (!databaseConfig.getProperties().containsKey(BUCKET)) {
      throw new IllegalArgumentException("Bucket name is not specified.");
    }
    if (!databaseConfig.getProperties().containsKey(STORAGE_TYPE)) {
      throw new IllegalArgumentException("Storage type is not specified.");
    }
    storage_type = databaseConfig.getProperties().getProperty(STORAGE_TYPE);
    if (!databaseConfig.getProperties().containsKey(BUCKET)) {
      throw new IllegalArgumentException("Bucket name is not specified.");
    }
    bucket = databaseConfig.getProperties().getProperty(BUCKET);

    if (databaseConfig.getProperties().containsKey(TABLE_METADATA_NAMESPACE)) {
      logger.warn(
          "The configuration property \""
              + TABLE_METADATA_NAMESPACE
              + "\" is deprecated and will be removed in 5.0.0.");

      metadataNamespace =
          getString(
              databaseConfig.getProperties(),
              TABLE_METADATA_NAMESPACE,
              DatabaseConfig.DEFAULT_SYSTEM_NAMESPACE_NAME);
    } else {
      metadataNamespace = databaseConfig.getSystemNamespaceName();
    }

    // For S3
    region = storage_type.equals(S3Wrapper.STORAGE_TYPE) ? endpoint : null;
    endpointOverride = getString(databaseConfig.getProperties(), ENDPOINT_OVERRIDE, null);

    // For Blob Storage
    blockSize = getLong(databaseConfig.getProperties(), BLOB_BLOCK_SIZE, 50 * 1024 * 1024);
    maxUploadConcurrency = getInt(databaseConfig.getProperties(), BLOB_MAX_UPLOAD_CONCURRENCY, 4);
    maxSingleUploadSize =
        getLong(databaseConfig.getProperties(), BLOB_MAX_SINGLE_UPLOAD_SIZE, 100 * 1024 * 1024);
    timeoutInSeconds = getInt(databaseConfig.getProperties(), BLOB_TIMEOUT_IN_SECONDS, 5);

    // For Cloud Storage
    projectId = getString(databaseConfig.getProperties(), PROJECT_ID, null);
    if (Objects.equals(storage_type, CloudStorageWrapper.STORAGE_TYPE) && projectId == null) {
      throw new IllegalArgumentException("Project ID is not specified.");
    }
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getStorageType() {
    return storage_type;
  }

  public String getBucket() {
    return bucket;
  }

  public String getMetadataNamespace() {
    return metadataNamespace;
  }

  // For S3

  public String getRegion() {
    return region;
  }

  public Optional<String> getEndpointOverride() {
    return Optional.ofNullable(endpointOverride);
  }

  // For Blob Storage

  public long getBlockSize() {
    return blockSize;
  }

  public int getMaxUploadConcurrency() {
    return maxUploadConcurrency;
  }

  public long getMaxSingleUploadSize() {
    return maxSingleUploadSize;
  }

  public int getTimeoutInSeconds() {
    return timeoutInSeconds;
  }

  // For Cloud Storage

  public String getProjectId() {
    return projectId;
  }
}
