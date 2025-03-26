package com.scalar.db.storage.objectstorage;

import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectStorageConfig {
  public static final String STORAGE_NAME = "object_storage";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  public static final String STORAGE_TYPE = PREFIX + "type";
  public static final String BUCKET = PREFIX + "bucket";

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
    storage_type = getString(databaseConfig.getProperties(), STORAGE_TYPE, null);
    bucket = getString(databaseConfig.getProperties(), BUCKET, null);

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
}
