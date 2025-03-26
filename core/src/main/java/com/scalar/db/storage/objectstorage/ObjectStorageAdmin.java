package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
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

public class ObjectStorageAdmin implements DistributedStorageAdmin {
  public static final String NAMESPACE_TABLE = "namespaces";
  public static final String METADATA_TABLE = "metadata";

  private final ObjectStorageWrapper wrapper;
  private final String metadataNamespace;

  @Inject
  public ObjectStorageAdmin(DatabaseConfig databaseConfig) {
    ObjectStorageConfig config = new ObjectStorageConfig(databaseConfig);
    wrapper = ObjectStorageUtils.getObjectStorageWrapper(config);
    metadataNamespace = config.getMetadataNamespace();
  }

  public ObjectStorageAdmin(ObjectStorageWrapper wrapper, ObjectStorageConfig config) {
    this.wrapper = wrapper;
    metadataNamespace = config.getMetadataNamespace();
  }

  @Override
  public TableMetadata getImportTableMetadata(
      String namespace, String table, Map<String, DataType> overrideColumnsType)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void addRawColumnToTable(
      String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void close() {
    wrapper.close();
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    try {
      insert(
          ObjectStorageUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, namespace),
          namespace,
          new ObjectStorageNamespaceMetadata(namespace));
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
      String concatenatedKey =
          String.join(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER, namespace, table);
      insert(
          ObjectStorageUtils.getObjectKey(metadataNamespace, METADATA_TABLE, concatenatedKey),
          concatenatedKey,
          new ObjectStorageTableMetadata(metadata));
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to create the table %s.%s", table, namespace), e);
    }
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    try {
      String concatenatedKey =
          String.join(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER, namespace, table);
      delete(
          ObjectStorageUtils.getObjectKey(metadataNamespace, METADATA_TABLE, concatenatedKey),
          concatenatedKey);
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to drop the table %s.%s", table, namespace), e);
    }
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    try {
      delete(
          ObjectStorageUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, namespace),
          namespace);
    } catch (Exception e) {
      throw new ExecutionException(String.format("Failed to drop the namespace %s", namespace), e);
    }
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    try {
      wrapper.getKeys(ObjectStorageUtils.getObjectKey(namespace, table, null)).stream()
          .map(
              key -> {
                List<String> parts =
                    Splitter.on(ObjectStorageUtils.OBJECT_KEY_DELIMITER).splitToList(key);
                return !parts.isEmpty() ? parts.get(parts.size() - 1) : "";
              })
          .filter(lastPart -> !lastPart.isEmpty())
          .forEach(
              partitionName -> {
                try {
                  wrapper.delete(ObjectStorageUtils.getObjectKey(namespace, table, partitionName));
                } catch (ObjectStorageWrapperException e) {
                  if (e.getCode() != ObjectStorageWrapperException.StatusCode.NOT_FOUND)
                    throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      throw new ExecutionException(
          String.format("Failed to truncate the table %s.%s", table, namespace), e);
    }
  }

  @Override
  public void createIndex(
      String namespace, String table, String columnName, Map<String, String> options)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_INDEX_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void dropIndex(String namespace, String table, String columnName)
      throws ExecutionException {
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_INDEX_NOT_SUPPORTED.buildMessage());
  }

  @Nullable
  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      String concatenatedKey =
          String.join(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER, namespace, table);
      ObjectStorageWrapperResponse response =
          wrapper.get(
              ObjectStorageUtils.getObjectKey(metadataNamespace, METADATA_TABLE, concatenatedKey));
      Map<String, ObjectStorageTableMetadata> metadataTable =
          JsonConvertor.deserialize(
              response.getValue(), new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});
      ObjectStorageTableMetadata metadata = metadataTable.get(concatenatedKey);
      return metadata != null ? metadata.toTableMetadata() : null;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // The metadata table does not exist
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
          .getKeys(ObjectStorageUtils.getObjectKey(metadataNamespace, METADATA_TABLE, null))
          .stream()
          .map(
              key -> {
                List<String> parts =
                    Splitter.on(ObjectStorageUtils.OBJECT_KEY_DELIMITER).splitToList(key);
                return !parts.isEmpty() ? parts.get(parts.size() - 1) : "";
              })
          .filter(lastPart -> !lastPart.isEmpty())
          .map(
              lastPart -> {
                List<String> parts =
                    Splitter.on(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER)
                        .splitToList(lastPart);
                return parts.size() > 1 && parts.get(0).equals(namespace) ? parts.get(1) : "";
              })
          .filter(tableName -> !tableName.isEmpty())
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
      ObjectStorageWrapperResponse response =
          wrapper.get(
              ObjectStorageUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, namespace));
      Map<String, ObjectStorageNamespaceMetadata> metadataTable =
          JsonConvertor.deserialize(
              response.getValue(),
              new TypeReference<Map<String, ObjectStorageNamespaceMetadata>>() {});
      return metadataTable.containsKey(namespace);
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
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
      upsert(
          ObjectStorageUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, namespace),
          namespace,
          new ObjectStorageNamespaceMetadata(namespace));
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
      String concatenatedKey =
          String.join(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER, namespace, table);
      upsert(
          ObjectStorageUtils.getObjectKey(metadataNamespace, METADATA_TABLE, concatenatedKey),
          concatenatedKey,
          new ObjectStorageTableMetadata(metadata));
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
      String concatenatedKey =
          String.join(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER, namespace, table);
      ObjectStorageWrapperResponse response =
          wrapper.get(
              ObjectStorageUtils.getObjectKey(metadataNamespace, METADATA_TABLE, concatenatedKey));
      Map<String, ObjectStorageTableMetadata> tableMetadataTable =
          JsonConvertor.deserialize(
              response.getValue(), new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});
      if (!tableMetadataTable.containsKey(
          String.join(ObjectStorageUtils.CONCATENATED_KEY_DELIMITER, namespace, table))) {
        // The metadata table does not exist
        throw new ExecutionException(
            String.format("The table %s.%s does not exist", table, namespace));
      }
      TableMetadata currentMetadata = tableMetadataTable.get(concatenatedKey).toTableMetadata();
      TableMetadata newMetadata =
          TableMetadata.newBuilder(currentMetadata).addColumn(columnName, columnType).build();
      tableMetadataTable.put(concatenatedKey, new ObjectStorageTableMetadata(newMetadata));
      if (!wrapper.updateIfVersionMatches(
          ObjectStorageUtils.getObjectKey(metadataNamespace, METADATA_TABLE, concatenatedKey),
          JsonConvertor.serialize(tableMetadataTable),
          response.getVersion())) {
        // If the metadata table is updated by another transaction, throw an exception
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
    throw new UnsupportedOperationException(
        CoreError.OBJECT_STORAGE_IMPORT_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public Set<String> getNamespaceNames() throws ExecutionException {
    try {
      return wrapper
          .getKeys(ObjectStorageUtils.getObjectKey(metadataNamespace, NAMESPACE_TABLE, null))
          .stream()
          .map(
              key -> {
                List<String> parts =
                    Splitter.on(ObjectStorageUtils.OBJECT_KEY_DELIMITER).splitToList(key);
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
      wrapper
          .getKeys(ObjectStorageUtils.getObjectKey(metadataNamespace, METADATA_TABLE, null))
          .stream()
          .map(
              key -> {
                List<String> parts =
                    Splitter.on(ObjectStorageUtils.OBJECT_KEY_DELIMITER).splitToList(key);
                return !parts.isEmpty() ? parts.get(0) : "";
              })
          .filter(lastPart -> !lastPart.isEmpty())
          .forEach(
              namespaceName -> {
                try {
                  upsert(
                      ObjectStorageUtils.getObjectKey(
                          metadataNamespace, NAMESPACE_TABLE, namespaceName),
                      namespaceName,
                      new ObjectStorageNamespaceMetadata(namespaceName));
                } catch (ExecutionException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      throw new ExecutionException("Failed to upgrade", e);
    }
  }

  private <T> void insert(String objectKey, String concatenatedKey, T metadata)
      throws ExecutionException {
    try {
      ObjectStorageWrapperResponse response = wrapper.get(objectKey);
      Map<String, T> metadataTable =
          JsonConvertor.deserialize(response.getValue(), new TypeReference<Map<String, T>>() {});
      if (metadataTable.containsKey(concatenatedKey)) {
        // If the metadata already exists in the metadata table, throw an exception
        throw new ExecutionException("The metadata already exists.");
      }
      metadataTable.put(concatenatedKey, metadata);
      if (!wrapper.updateIfVersionMatches(
          objectKey, JsonConvertor.serialize(metadataTable), response.getVersion())) {
        // If the metadata is updated by another transaction, throw an exception
        throw new ExecutionException("Failed to insert the metadata due to a conflict.");
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // The metadata table does not exist. Create a new metadata table.
        try {
          wrapper.insert(
              objectKey,
              JsonConvertor.serialize(Collections.singletonMap(concatenatedKey, metadata)));
        } catch (ObjectStorageWrapperException e2) {
          throw new ExecutionException("Failed to insert the metadata.", e2);
        }
      } else {
        throw new ExecutionException("Failed to insert the metadata.", e);
      }
    } catch (Exception e) {
      throw new ExecutionException("Failed to insert the metadata.", e);
    }
  }

  private <T> void upsert(String objectKey, String concatenatedKey, T metadata)
      throws ExecutionException {
    try {
      ObjectStorageWrapperResponse response = wrapper.get(objectKey);
      Map<String, T> metadataTable =
          JsonConvertor.deserialize(response.getValue(), new TypeReference<Map<String, T>>() {});
      // Upsert the metadata
      metadataTable.put(concatenatedKey, metadata);
      if (!wrapper.updateIfVersionMatches(
          objectKey, JsonConvertor.serialize(metadataTable), response.getVersion())) {
        // If the metadata is updated by another transaction, throw an exception
        throw new ExecutionException("Failed to upsert the metadata due to a conflict.");
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // The metadata table does not exist. Create a new metadata table.
        try {
          wrapper.insert(
              objectKey,
              JsonConvertor.serialize(Collections.singletonMap(concatenatedKey, metadata)));
        } catch (ObjectStorageWrapperException e2) {
          throw new ExecutionException("Failed to upsert the metadata.", e2);
        }
      } else {
        throw new ExecutionException("Failed to upsert the metadata.", e);
      }
    } catch (Exception e) {
      throw new ExecutionException("Failed to upsert the metadata.", e);
    }
  }

  private <T> void delete(String objectKey, String concatenatedKey) throws ExecutionException {
    try {
      ObjectStorageWrapperResponse response = wrapper.get(objectKey);
      Map<String, T> metadataTable =
          JsonConvertor.deserialize(response.getValue(), new TypeReference<Map<String, T>>() {});
      if (!metadataTable.containsKey(concatenatedKey)) {
        // If the metadata does not exist in the table, throw an exception
        throw new ExecutionException("The metadata does not exist.");
      }
      metadataTable.remove(concatenatedKey);
      if (metadataTable.isEmpty()) {
        // If the metadata table is empty, delete the metadata table
        if (!wrapper.deleteIfVersionMatches(objectKey, response.getVersion())) {
          // If the metadata table is updated by another transaction, throw an exception
          throw new ExecutionException("Failed to delete the metadata due to a conflict.");
        }
      } else {
        if (!wrapper.updateIfVersionMatches(
            objectKey, JsonConvertor.serialize(metadataTable), response.getVersion())) {
          // If the metadata table is updated by another transaction, throw an exception
          throw new ExecutionException("Failed to delete the metadata due to a conflict.");
        }
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // The metadata table does not exist.
        throw new ExecutionException("The metadata does not exist.");
      }
      throw new ExecutionException("Failed to delete the metadata.", e);
    } catch (Exception e) {
      throw new ExecutionException("Failed to delete the metadata.", e);
    }
  }
}
