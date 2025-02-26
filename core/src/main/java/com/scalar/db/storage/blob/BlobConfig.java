package com.scalar.db.storage.blob;

import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobConfig {
  public static final String STORAGE_NAME = "blob";
  public static final String PREFIX = DatabaseConfig.PREFIX + STORAGE_NAME + ".";
  public static final String BUCKET = PREFIX + "container";

  /** @deprecated As of 5.0, will be removed. */
  @Deprecated
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";

  private static final Logger logger = LoggerFactory.getLogger(BlobConfig.class);
  private final String endpoint;
  private final String accountName;
  private final String accountKey;
  private final String container;
  private final String metadataNamespace;

  public BlobConfig(DatabaseConfig databaseConfig) {
    String storage = databaseConfig.getStorage();
    if (!storage.equals(STORAGE_NAME)) {
      throw new IllegalArgumentException(
          DatabaseConfig.STORAGE + " should be '" + STORAGE_NAME + "'");
    }

    if (databaseConfig.getContactPoints().isEmpty()) {
      throw new IllegalArgumentException(CoreError.INVALID_CONTACT_POINTS.buildMessage());
    }
    endpoint = databaseConfig.getContactPoints().get(0);
    accountName = databaseConfig.getUsername().orElse(null);
    accountKey = databaseConfig.getPassword().orElse(null);
    if (!databaseConfig.getProperties().containsKey(BUCKET)) {
      throw new IllegalArgumentException("Bucket name is not specified.");
    }
    container = getString(databaseConfig.getProperties(), BUCKET, null);

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

  public String getAccountName() {
    return accountName;
  }

  public String getAccountKey() {
    return accountKey;
  }

  public String getContainer() {
    return container;
  }

  public String getMetadataNamespace() {
    return metadataNamespace;
  }
}
