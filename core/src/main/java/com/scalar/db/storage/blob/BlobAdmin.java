package com.scalar.db.storage.blob;

import com.google.common.base.Splitter;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class BlobAdmin implements DistributedStorageAdmin {
  public static final String NAMESPACE_TABLE = "namespaces";
  public static final String METADATA_TABLE = "metadata";

  private final BlobClientWrapper wrapper;
  private final String metadataNamespace;

  @Inject
  public BlobAdmin(DatabaseConfig databaseConfig) {
    BlobConfig config = new BlobConfig(databaseConfig);
    wrapper = new BlobClientWrapper(BlobUtils.buildBlobContainerClient(config));
    metadataNamespace = config.getMetadataNamespace();
  }

  public BlobAdmin(BlobClientWrapper wrapper, BlobConfig config) {
    this.wrapper = wrapper;
    metadataNamespace = config.getMetadataNamespace();
  }

  @Override
  public TableMetadata getImportTableMetadata(
      String namespace, String table, Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.BLOB_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.BLOB_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void close() {
    wrapper.close();
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      insertNamespace(namespace);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to create the namespace %s", namespace), e);
    }
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      insertTableMetadata(namespace, table, metadata);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to create the table %s.%s", table, namespace), e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    try {
      deleteTableMetadata(namespace, table);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to drop the table %s.%s", table, namespace), e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try {
      deleteNamespace(namespace);
    } catch (Exception e) {
      throw new ExecutionException(String.format("Failed to drop the namespace %s", namespace), e);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    try {
      Set<String> keys = wrapper.listKeys(BlobUtils.getObjectKey(namespace, table, null, null));
      for (String key : keys) {
        wrapper.deleteIfExists(key);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to truncate the table %s.%s", table, namespace), e);
    }
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.BLOB_INDEX_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.BLOB_INDEX_NOT_SUPPORTED.buildMessage());
  }

  @Nullable
  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      BlobClientWrapperResponse response =
          wrapper.get(BlobUtils.getObjectKey(metadataNamespace, METADATA_TABLE, namespace, table));
      return JsonConvertor.deserialize(response.getValue(), BlobTableMetadata.class)
          .toTableMetadata();
    } catch (BlobClientWrapperException e) {
      if (e.getCode() == BlobClientWrapperException.StatusCode.NOT_FOUND) {
        return null;
      }
      throw new ExecutionException(
          String.format("Failed to get the metadata of the table %s.%s", table, namespace), e);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to get the metadata of the table %s.%s", table, namespace), e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    try {
      if (!namespaceExists(namespace)) {
        return Collections.emptySet();
      }
      return wrapper
          .listKeys(BlobUtils.getObjectKey(metadataNamespace, METADATA_TABLE, namespace, null))
          .stream()
          .map(
              key -> {
                List<String> parts = Splitter.on(BlobUtils.OBJECT_KEY_DELIMITER).splitToList(key);
                return !parts.isEmpty() ? parts.get(parts.size() - 1) : "";
              })
          .filter(lastPart -> !lastPart.isEmpty())
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to get the table names of the namespace %s", namespace), e);
    }
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    if (metadataNamespace.equals(namespace)) {
      return true;
    }
    try {
      wrapper.get(BlobUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, namespace, namespace));
      return true;
    } catch (BlobClientWrapperException e) {
      if (e.getCode() == BlobClientWrapperException.StatusCode.NOT_FOUND) {
        return false;
      }
      throw new ExecutionException(
          String.format("Failed to check the existence of the namespace %s", namespace), e);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to check the existence of the namespace %s", namespace), e);
    }
  }

  @Override
  public void repairNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      upsertNamespace(namespace);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to repair the namespace %s", namespace), e);
    }
  }

  @Override
  public void repairTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    try {
      upsertTableMetadata(namespace, table, metadata);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to repair the table %s.%s", table, namespace), e);
    }
  }

  @Override
  public void addNewColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    try {
      BlobClientWrapperResponse response =
          wrapper.get(BlobUtils.getObjectKey(metadataNamespace, METADATA_TABLE, namespace, table));
      TableMetadata currentMetadata =
          JsonConvertor.deserialize(response.getValue(), BlobTableMetadata.class).toTableMetadata();
      TableMetadata newMetadata =
          TableMetadata.newBuilder(currentMetadata).addColumn(columnName, columnType).build();
      if (!wrapper.compareAndSwap(
          BlobUtils.getObjectKey(metadataNamespace, METADATA_TABLE, namespace, table),
          JsonConvertor.serialize(new BlobTableMetadata(newMetadata)),
          response.getETag())) {
        throw new ExecutionException(
            String.format(
                "Failed to add the column %s to the table %s.%s due to a conflict",
                columnName, table, namespace));
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (Exception e) {
      throw new ExecutionException(
          String.format(
              "Failed to add the column %s to the table %s.%s", columnName, table, namespace),
          e);
    }
  }

  @Override
  public void importTable(
      String namespace,
      String table,
      Map<String, String> options,
      Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    throw new UnsupportedOperationException(CoreError.BLOB_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      return wrapper
          .listKeys(BlobUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, null, null)).stream()
          .map(
              key -> {
                List<String> parts = Splitter.on(BlobUtils.OBJECT_KEY_DELIMITER).splitToList(key);
                return !parts.isEmpty() ? parts.get(parts.size() - 1) : "";
              })
          .filter(lastPart -> !lastPart.isEmpty())
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new ExecutionException("Failed to get the namespace names", e);
    }
  }

  @Override
  public void upgrade(Map<String, String> options) throws ExecutionException {
    try {
      wrapper.listKeys(BlobUtils.getObjectKey(metadataNamespace, METADATA_TABLE, null, null))
          .stream()
          .map(
              key -> {
                List<String> parts = Splitter.on(BlobUtils.OBJECT_KEY_DELIMITER).splitToList(key);
                return !parts.isEmpty() ? parts.get(0) : "";
              })
          .filter(lastPart -> !lastPart.isEmpty())
          .forEach(
              namespaceName -> {
                try {
                  upsertNamespace(namespaceName);
                } catch (ExecutionException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      throw new ExecutionException("Failed to upgrade", e);
    }
  }

  private void insertTableMetadata(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    try {
      wrapper.insert(
          BlobUtils.getObjectKey(metadataNamespace, METADATA_TABLE, namespace, table),
          JsonConvertor.serialize(new BlobTableMetadata(metadata)));
    } catch (Exception e) {
      throw new ExecutionException("Failed to insert the table metadata", e);
    }
  }

  private void upsertTableMetadata(String namespace, String table, TableMetadata metadata)
      throws ExecutionException {
    String objectKey = BlobUtils.getObjectKey(metadataNamespace, METADATA_TABLE, namespace, table);
    try {
      BlobClientWrapperResponse response = wrapper.get(objectKey);
      if (!wrapper.compareAndSwap(
          objectKey,
          JsonConvertor.serialize(new BlobTableMetadata(metadata)),
          response.getETag())) {
        throw new ExecutionException(
            String.format(
                "Failed to upsert the table metadata %s.%s due to a conflict", table, namespace));
      }
    } catch (BlobClientWrapperException e) {
      if (e.getCode() == BlobClientWrapperException.StatusCode.NOT_FOUND) {
        try {
          wrapper.insert(objectKey, JsonConvertor.serialize(new BlobTableMetadata(metadata)));
        } catch (Exception e2) {
          throw new ExecutionException("Failed to upsert the table metadata", e2);
        }
      } else {
        throw new ExecutionException("Failed to upsert the table metadata", e);
      }
    } catch (Exception e) {
      throw new ExecutionException("Failed to upsert the table metadata", e);
    }
  }

  private void deleteTableMetadata(String namespace, String table) throws ExecutionException {
    String objectKey = BlobUtils.getObjectKey(metadataNamespace, METADATA_TABLE, namespace, table);
    try {
      wrapper.deleteIfExists(objectKey);
    } catch (Exception e) {
      throw new ExecutionException("Failed to delete the table metadata", e);
    }
  }

  private void insertNamespace(String namespace) throws ExecutionException {
    String objectKey =
        BlobUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, namespace, namespace);
    try {
      wrapper.insert(objectKey, JsonConvertor.serialize(new BlobNamespace(namespace)));
    } catch (Exception e) {
      throw new ExecutionException("Failed to insert the namespace", e);
    }
  }

  private void upsertNamespace(String namespace) throws ExecutionException {
    String objectKey =
        BlobUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, namespace, namespace);
    try {
      BlobClientWrapperResponse response = wrapper.get(objectKey);
      if (!wrapper.compareAndSwap(
          objectKey, JsonConvertor.serialize(new BlobNamespace(namespace)), response.getETag())) {
        throw new ExecutionException(
            String.format("Failed to upsert the namespace %s due to a conflict", namespace));
      }
    } catch (BlobClientWrapperException e) {
      if (e.getCode() == BlobClientWrapperException.StatusCode.NOT_FOUND) {
        try {
          wrapper.insert(objectKey, JsonConvertor.serialize(new BlobNamespace(namespace)));
        } catch (Exception e2) {
          throw new ExecutionException("Failed to upsert the namespace", e2);
        }
      } else {
        throw new ExecutionException("Failed to upsert the namespace", e);
      }
    } catch (Exception e) {
      throw new ExecutionException("Failed to upsert the namespace", e);
    }
  }

  private void deleteNamespace(String namespace) throws ExecutionException {
    String objectKey =
        BlobUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, namespace, namespace);
    try {
      wrapper.deleteIfExists(objectKey);
    } catch (Exception e) {
      throw new ExecutionException("Failed to delete the namespace", e);
    }
  }
}
