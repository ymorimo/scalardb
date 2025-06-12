package com.scalar.db.dataloader.cli.command.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

public class ImportCommandCsvPostgresSQLIT {
  private static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:15")
          .withDatabaseName("scalardb")
          .withUsername("postgres")
          .withPassword("12345678");

  private Path configFilePath;

  @TempDir Path tempDir;

  @BeforeAll
  static void startContainers() {
    postgres.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_postgres_import.sql"),
        "/docker-entrypoint-initdb.d/init_postgres_import.sql");
    postgres.start();
  }

  @AfterAll
  static void stopContainers() {
    if (postgres != null) {
      postgres.stop();
    }
  }

  @BeforeEach
  void setup() throws Exception {
    configFilePath = tempDir.resolve("scalardb.properties");
    FileUtils.writeStringToFile(configFilePath.toFile(), getScalarDbConfig(), "UTF-8");
  }

  private static String getScalarDbConfig() {
    return new StringBuilder()
        .append("scalar.db.storage=jdbc\n")
        .append("scalar.db.contact_points=")
        .append(postgres.getJdbcUrl())
        .append("\n")
        .append("scalar.db.username=")
        .append(postgres.getUsername())
        .append("\n")
        .append("scalar.db.password=")
        .append(postgres.getPassword())
        .append("\n")
        .append("scalar.db.jdbc.driver_class=org.postgresql.Driver\n")
        .append("scalar.db.namespace=test\n")
        .append("scalar.db.cross_partition_scan.enabled=true\n")
        .toString();
  }

  private Path resourcePath(String relativePath) throws Exception {
    return Paths.get(
        Objects.requireNonNull(getClass().getClassLoader().getResource(relativePath)).toURI());
  }

  private int executeImportCommand(String[] args) {
    return new CommandLine(new ImportCommand()).execute(args);
  }

  @Test
  void testImportWithTransactionAndChunkAndSize() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee_trn",
      "--log-dir", resourcePath("logs").toString(),
      "--format", "CSV",
      "--import-mode", "UPSERT",
      "--mode", "TRANSACTION",
      "--file", resourcePath("import_data/import_single_mapped.csv").toString(),
      "--control-file", resourcePath("control_files/control_file_trn_mapped.json").toString(),
      "--control-file-validation", "MAPPED",
      "--log-raw-record", "--log-success",
      "--transaction-size", "1",
      "--data-chunk-size", "5"
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testImportWithTransactionAndSplitLog() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee_trn",
      "--log-dir", resourcePath("logs").toString(),
      "--format", "CSV",
      "--import-mode", "UPSERT",
      "--mode", "TRANSACTION",
      "--file", resourcePath("import_data/import_single_mapped.csv").toString(),
      "--control-file", resourcePath("control_files/control_file_trn_mapped.json").toString(),
      "--control-file-validation", "MAPPED",
      "--log-raw-record", "--log-success",
      "--split-log-mode"
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testMultiTableStorageCsvUpsert() throws Exception {
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "employee",
      "--log-dir",
      tempDir.toString(),
      "--format",
      "CSV",
      "--import-mode",
      "UPSERT",
      "--file",
      resourcePath("import_data/import_multi_mapped.csv").toString(),
      "--control-file",
      resourcePath("control_files/control_file_multi.json").toString(),
      "--control-file-validation",
      "FULL",
      "--log-success"
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testCsvInsertAllColumns() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--log-dir", tempDir.toString(),
      "--format", "CSV",
      "--import-mode", "INSERT",
      "--file", resourcePath("import_data/import_data_all.csv").toString()
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testCsvUpdateAllColumns() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--log-dir", tempDir.toString(),
      "--format", "CSV",
      "--import-mode", "UPDATE",
      "--file", resourcePath("import_data/import_data_all.csv").toString()
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testCsvUpsertAllColumns() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--log-dir", tempDir.toString(),
      "--format", "CSV",
      "--import-mode", "UPSERT",
      "--file", resourcePath("import_data/import_data_all.csv").toString()
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testCsvTransactionControlFileFull() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee_trn",
      "--log-dir", tempDir.toString(),
      "--format", "CSV",
      "--mode", "TRANSACTION",
      "--import-mode", "UPSERT",
      "--file", resourcePath("import_data/import_single_trn_full.csv").toString(),
      "--control-file", resourcePath("control_files/control_file_trn.json").toString(),
      "--control-file-validation", "FULL",
      "--log-success"
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testCsvTransactionWithLogRawRecords() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee_trn",
      "--log-dir", resourcePath("logs").toString(),
      "--format", "CSV",
      "--import-mode", "UPSERT",
      "--mode", "TRANSACTION",
      "--file", resourcePath("import_data/import_single_trn_full.csv").toString(),
      "--control-file", resourcePath("control_files/control_file_trn.json").toString(),
      "--control-file-validation", "MAPPED",
      "--log-raw-record", "--log-success"
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testCsvUpsertWithoutHeader() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "CSV",
      "--import-mode", "UPSERT",
      "--file", resourcePath("import_data/import_single_without_header.csv").toString(),
      "--delimiter", ",",
      "--header", "id,name,email"
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testCsvUpdateWithoutHeader() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "CSV",
      "--import-mode", "UPDATE",
      "--file", resourcePath("import_data/import_single_without_header.csv").toString(),
      "--delimiter", ",",
      "--header", "id,name,email"
    };
    assertEquals(0, executeImportCommand(args));
  }

  @Test
  void testCsvUpsertWithDifferentDelimiter() throws Exception {
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "CSV",
      "--import-mode", "UPSERT",
      "--file", resourcePath("import_data/import_single_delimiter.csv").toString(),
      "--delimiter", ":"
    };
    assertEquals(0, executeImportCommand(args));
  }
}
