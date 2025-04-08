package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.AdminTestUtils;
import java.util.Map;
import java.util.Properties;

public class ObjectStorageAdminTestUtils extends AdminTestUtils {
  private final ObjectStorageWrapper wrapper;
  private final String metadataNamespace;

  public ObjectStorageAdminTestUtils(Properties properties) {
    super(properties);
    ObjectStorageConfig config = new ObjectStorageConfig(new DatabaseConfig(properties));
    wrapper = ObjectStorageUtils.getObjectStorageWrapper(config);
    metadataNamespace = config.getMetadataNamespace();
  }

  @Override
  public void dropNamespacesTable() throws Exception {
    // Do nothing
    // Object storages do not have a concept of table
  }

  @Override
  public void dropMetadataTable() throws Exception {
    // Do nothing
    // Object storages do not have a concept of table
  }

  @Override
  public void truncateNamespacesTable() throws Exception {
    wrapper.delete(
        ObjectStorageUtils.getObjectKey(
            metadataNamespace, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE, null));
  }

  @Override
  public void truncateMetadataTable() throws Exception {
    wrapper.delete(
        ObjectStorageUtils.getObjectKey(
            metadataNamespace, ObjectStorageAdmin.TABLE_METADATA_TABLE, null));
  }

  @Override
  public void corruptMetadata(String namespace, String table) throws Exception {
    String objectKey =
        ObjectStorageUtils.getObjectKey(
            metadataNamespace, ObjectStorageAdmin.TABLE_METADATA_TABLE, null);
    ObjectStorageWrapperResponse response = wrapper.get(objectKey);
    Map<String, ObjectStorageTableMetadata> metadataTable =
        JsonConvertor.deserialize(
            response.getValue(), new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});
    metadataTable.put(
        ObjectStorageUtils.getObjectKey(namespace, table, null), new ObjectStorageTableMetadata());
    if (!wrapper.updateIfVersionMatches(
        objectKey, JsonConvertor.serialize(metadataTable), response.getVersion())) {
      throw new IllegalStateException("Failed to update metadata");
    }
  }

  @Override
  public void dropNamespace(String namespace) throws Exception {
    // Do nothing
    // Object storages do not have a concept of namespace
  }

  @Override
  public boolean namespaceExists(String namespace) throws Exception {
    // Object storages do not have a concept of namespace
    return true;
  }

  @Override
  public boolean tableExists(String namespace, String table) throws Exception {
    // Object storages do not have a concept of table
    return true;
  }

  @Override
  public void dropTable(String namespace, String table) throws Exception {
    // Do nothing
    // Object storages do not have a concept of table
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }
}
