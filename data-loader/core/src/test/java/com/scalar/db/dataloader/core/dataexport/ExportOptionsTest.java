package com.scalar.db.dataloader.core.dataexport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.io.Key;
import org.junit.jupiter.api.Test;

class ExportOptionsTest {

  @Test
  void builder_withRequiredFields_shouldCreateExportOptions() {
    // Arrange
    String namespace = "test_namespace";
    String tableName = "test_table";
    Key scanPartitionKey = Key.ofInt("column", 1);
    FileFormat outputFileFormat = FileFormat.JSON;

    // Act
    ExportOptions exportOptions =
        ExportOptions.builder(namespace, tableName, scanPartitionKey, outputFileFormat).build();

    // Assert
    assertEquals(exportOptions.getNamespace(), namespace);
    assertEquals(exportOptions.getTableName(), tableName);
    assertEquals(exportOptions.getScanPartitionKey(), scanPartitionKey);
    assertEquals(exportOptions.getOutputFileFormat(), outputFileFormat);
  }

  @Test
  void builder_withDefaultValues_shouldSetDefaultValues() {
    // Arrange
    String namespace = "test_namespace";
    String tableName = "test_table";
    Key scanPartitionKey = Key.ofInt("column", 1);
    FileFormat outputFileFormat = FileFormat.JSON;

    // Act
    ExportOptions exportOptions =
        ExportOptions.builder(namespace, tableName, scanPartitionKey, outputFileFormat).build();

    // Assert
    assertThat(exportOptions.getDataChunkSize()).isEqualTo(Integer.MAX_VALUE);
    assertThat(exportOptions.getDelimiter()).isEqualTo(",");
    assertThat(exportOptions.isExcludeHeaderRow()).isFalse();
    assertThat(exportOptions.isIncludeTransactionMetadata()).isFalse();
    assertThat(exportOptions.getProjectionColumns()).isEmpty();
  }
}
