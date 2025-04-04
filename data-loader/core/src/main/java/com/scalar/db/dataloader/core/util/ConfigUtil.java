package com.scalar.db.dataloader.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {
  public static final String CONFIG_PROPERTIES = "config.properties";
  private static volatile Properties properties;

  private static void ensurePropertiesLoaded() {
    if (properties == null) {
      synchronized (ConfigUtil.class) {
        if (properties == null) {
          loadProperties();
        }
      }
    }
  }

  private static void loadProperties() {
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_PROPERTIES)) {
      if (inputStream == null) {
        throw new RuntimeException("config.properties file not found in classpath.");
      }
      properties = new Properties();
      properties.load(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load config.properties", e);
    }
  }

  public static Integer getImportDataChunkQueueSize() {
    ensurePropertiesLoaded();
    return getIntegerProperty("import.data.chunk.queue.size", 256);
  }

  public static Integer getTransactionBatchThreadPoolSize() {
    ensurePropertiesLoaded();
    return getIntegerProperty("transaction.batch.thread.pool.size", 16);
  }

  private static Integer getIntegerProperty(String key, int defaultValue) {
    String value = properties.getProperty(key);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid integer value for property: " + key, e);
    }
  }
}
