package com.scalar.db.schemaloader;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.DataType;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ImportTableSchema {
  private final String OVERRIDE_COLUMNS_TYPE = "override-columns-type";
  private final String namespace;
  private final String tableName;
  private final boolean isTransactionTable;
  private final ImmutableMap<String, String> options;
  private final ImmutableMap<String, DataType> columns;

  public ImportTableSchema(
      String tableFullName, JsonObject tableDefinition, Map<String, String> options) {
    String[] fullName = tableFullName.split("\\.", -1);
    if (fullName.length != 2) {
      throw new IllegalArgumentException(
          CoreError.SCHEMA_LOADER_PARSE_ERROR_TABLE_NAME_MUST_CONTAIN_NAMESPACE_AND_TABLE
              .buildMessage(tableFullName));
    }
    namespace = fullName[0];
    tableName = fullName[1];
    if (tableDefinition.keySet().contains(TableSchema.TRANSACTION)) {
      isTransactionTable = tableDefinition.get(TableSchema.TRANSACTION).getAsBoolean();
    } else {
      isTransactionTable = true;
    }
    JsonObject columns = tableDefinition.get(OVERRIDE_COLUMNS_TYPE).getAsJsonObject();
    ImmutableMap.Builder<String, DataType> columnsBuilder = ImmutableMap.builder();
    for (Entry<String, JsonElement> column : columns.entrySet()) {
      String columnName = column.getKey();

      String columnDataType = column.getValue().getAsString().trim();

      DataType dataType = TableSchema.DATA_MAP_TYPE.get(columnDataType.toUpperCase());
      if (dataType == null) {
        throw new IllegalArgumentException(
            CoreError.SCHEMA_LOADER_PARSE_ERROR_INVALID_COLUMN_TYPE.buildMessage(
                tableFullName, columnName, column.getValue().getAsString()));
      }
      columnsBuilder.put(columnName, dataType);
    }
    this.columns = columnsBuilder.buildKeepingLast();
    this.options = buildOptions(tableDefinition, options);
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  private ImmutableMap<String, String> buildOptions(
      JsonObject tableDefinition, Map<String, String> globalOptions) {
    ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.builder();
    optionsBuilder.putAll(globalOptions);
    Set<String> keysToIgnore =
        ImmutableSet.of(
            TableSchema.PARTITION_KEY,
            TableSchema.CLUSTERING_KEY,
            TableSchema.TRANSACTION,
            TableSchema.COLUMNS,
            TableSchema.SECONDARY_INDEX);
    tableDefinition.entrySet().stream()
        .filter(entry -> !keysToIgnore.contains(entry.getKey()))
        .forEach(entry -> optionsBuilder.put(entry.getKey(), entry.getValue().getAsString()));
    // If an option is defined globally and in the JSON file, the JSON file value is used
    return optionsBuilder.buildKeepingLast();
  }

  public String getNamespace() {
    return namespace;
  }

  public String getTable() {
    return tableName;
  }

  public boolean isTransactionTable() {
    return isTransactionTable;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public Map<String, DataType> getColumns() {
    return columns;
  }
}
