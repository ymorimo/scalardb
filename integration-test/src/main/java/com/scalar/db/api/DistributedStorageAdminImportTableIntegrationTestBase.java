package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageAdminImportTableIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageAdminImportTableIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_admin_import_table";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private final List<TestData> testDataList = new ArrayList<>();
  protected DistributedStorageAdmin admin;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  private void dropTable() throws Exception {
    for (TestData testData : testDataList) {
      if (testData.getExpectedTableMetadata() == null) {
        dropNonImportableTable(testData.getTableName());
      } else {
        admin.dropTable(getNamespace(), testData.getTableName());
      }
    }
    if (!admin.namespaceExists(getNamespace())) {
      // Create metadata to be able to delete the namespace using the Admin
      admin.repairNamespace(getNamespace(), getCreationOptions());
    }
    admin.dropNamespace(getNamespace());
  }

  @BeforeEach
  protected void setUp() throws Exception {
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getStorageAdmin();
  }

  @AfterEach
  protected void afterEach() {
    try {
      dropTable();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }
  }

  @AfterAll
  protected void afterAll() throws Exception {}

  protected abstract List<TestData> createExistingDatabaseWithAllDataTypes(
      boolean createUnsupportedTables) throws SQLException;

  protected abstract void dropNonImportableTable(String table) throws Exception;

  @Test
  public void importTable_ShouldWorkProperly() throws Exception {
    // Arrange
    testDataList.addAll(createExistingDatabaseWithAllDataTypes(true));

    // Act Assert
    for (TestData testData : testDataList) {
      if (testData.getExpectedTableMetadata() == null) {
        importTable_ForNonImportableTable_ShouldThrowIllegalArgumentException(
            testData.getTableName());
      } else {
        importTable_ForImportableTable_ShouldImportProperly(
            testData.getTableName(),
            testData.getOverrideColumnsType(),
            testData.getExpectedTableMetadata());
      }
    }
    importTable_ForNonExistingTable_ShouldThrowIllegalArgumentException();
  }

  @Test
  public void importTable_ForUnsupportedDatabase_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    // Act Assert
    assertThatThrownBy(
            () -> admin.importTable(getNamespace(), "unsupported_db", Collections.emptyMap()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private void importTable_ForImportableTable_ShouldImportProperly(
      String table, Map<String, DataType> overrideColumnsType, TableMetadata metadata)
      throws ExecutionException {
    // Act
    admin.importTable(getNamespace(), table, Collections.emptyMap(), overrideColumnsType);

    // Assert
    assertThat(admin.namespaceExists(getNamespace())).isTrue();
    assertThat(admin.tableExists(getNamespace(), table)).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), table)).isEqualTo(metadata);
  }

  private void importTable_ForNonImportableTable_ShouldThrowIllegalArgumentException(String table) {
    // Act Assert
    if (table.equalsIgnoreCase("bad_table8")) {
      System.out.println("bad_table8");
    }
    assertThatThrownBy(
            () -> admin.importTable(getNamespace(), table, Collections.emptyMap()),
            "non-importable data type test failed: " + table)
        .isInstanceOf(IllegalArgumentException.class);
  }

  private void importTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Act Assert
    assertThatThrownBy(
            () -> admin.importTable(getNamespace(), "non-existing-table", Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class);
  }
    private void importTable_ForImportedTable_ShouldInsertThenGetCorrectly(
       String table, Map<String, DataType> overrideColumnsType,TableMetadata metadata, List<Column<?>> insertColumns )
      throws ExecutionException {
    // Arrange
    admin.importTable(getNamespace(), table, Collections.emptyMap(), overrideColumnsType);
    Insert insert = Insert.newBuilder().namespace(getNamespace()).table(table).partitionKey(Key.of()).build();
    // Act


    // Assert
    assertThat(admin.namespaceExists(getNamespace())).isTrue();
    assertThat(admin.tableExists(getNamespace(), table)).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), table)).isEqualTo(metadata);
  }


  public static class TestData {
    private final String tableName;
    private final Map<String, DataType> overrideColumnsType;
    private final @Nullable TableMetadata expectedTableMetadata;
    private final @Nullable List<Column<?>> insertColumns;
    public TestData(
        String tableName,
        Map<String, DataType> overrideColumnsType,
        @Nullable TableMetadata expectedTableMetadata, @Nullable List<Column<?>> columns) {
      this.tableName = tableName;
      this.overrideColumnsType = overrideColumnsType;
      this.expectedTableMetadata = expectedTableMetadata;
      this.insertColumns = columns;
    }

    public String getTableName() {
      return tableName;
    }

    public Map<String, DataType> getOverrideColumnsType() {
      return overrideColumnsType;
    }

    @Nullable
    public TableMetadata getExpectedTableMetadata() {
      return expectedTableMetadata;
    }

    @Nullable
    public List<Column<?>> getInsertColumns() {
      return insertColumns;
    }
  }
}
