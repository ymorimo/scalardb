package com.scalar.db.api;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.Map;

/**
 * An administrative interface for distributed storage implementations. The user can execute
 * administrative operations with it like createNamespace/createTable/getTableMetadata.
 *
 * <h3>Usage Examples</h3>
 *
 * Here is a simple example to demonstrate how to use it. (Exception handling is omitted for
 * readability.)
 *
 * <pre>{@code
 * StorageFactory factory = StorageFactory.create(configFilePath);
 * DistributedStorageAdmin admin = factory.getAdmin();
 *
 * // Create a namespace
 * // Assumes that the namespace name is NAMESPACE
 * admin.createNamespace(NAMESPACE);
 *
 * // Create a table
 * // Assumes that the namespace name and the table name are NAMESPACE and TABLE respectively
 * TableMetadata tableMetadata = TableMetadata.newBuilder()
 *     ...
 *     .build();
 * admin.createTable(NAMESPACE, TABLE, tableMetadata);
 *
 * // Get a table metadata
 * tableMetadata = admin.getTableMetadata(NAMESPACE, TABLE);
 *
 * // Truncate a table
 * admin.truncateTable(NAMESPACE, TABLE);
 *
 * // Drop a table
 * admin.dropTable(NAMESPACE, TABLE);
 *
 * // Drop a namespace
 * admin.dropNamespace(NAMESPACE);
 * }</pre>
 */
public interface DistributedStorageAdmin extends Admin, AutoCloseable {

  /**
   * Get import table metadata in the ScalarDB format.
   *
   * @param namespace namespace name of import table
   * @param table import table name
   * @throws IllegalArgumentException if the table does not exist
   * @throws IllegalStateException if the table does not meet the requirement of ScalarDB table
   * @throws ExecutionException if the operation fails
   * @return import table metadata in the ScalarDB format
   */
  default TableMetadata getImportTableMetadata(String namespace, String table)
      throws ExecutionException {
    return getImportTableMetadata(namespace, table, Collections.emptyMap());
  }

  /**
   * Get import table metadata in the ScalarDB format.
   *
   * @param namespace namespace name of import table
   * @param table import table name
   * @param overrideColumnsType a map of column data type by column name. Only set the column for
   *     which you want to override the default data type mapping.
   * @throws IllegalArgumentException if the table does not exist
   * @throws IllegalStateException if the table does not meet the requirement of ScalarDB table
   * @throws ExecutionException if the operation fails
   * @return import table metadata in the ScalarDB format
   */
  TableMetadata getImportTableMetadata(
      String namespace, String table, Map<String, DataType> overrideColumnsType)
      throws ExecutionException;

  /**
   * Add a column in the table without updating the metadata table in ScalarDB.
   *
   * @param namespace namespace name of import table
   * @param table import table name
   * @param columnName name of the column to be added
   * @param columnType type of the column to be added
   * @throws IllegalArgumentException if the table does not exist
   * @throws ExecutionException if the operation fails
   */
  void addRawColumnToTable(String namespace, String table, String columnName, DataType columnType)
      throws ExecutionException;

  /**
   * Returns the storage information.
   *
   * @param namespace the namespace to get the storage information for
   * @return the storage information
   * @throws ExecutionException if the operation fails
   */
  StorageInfo getStorageInfo(String namespace) throws ExecutionException;

  /** Closes connections to the storage. */
  @Override
  void close();
}
