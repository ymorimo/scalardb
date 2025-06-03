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

public class ImportCommandJsonPostgresSQLIT {

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
        "/docker-entrypoint-initdb.d/init_postgres_import.sql"); // Ensures the SQL file is
    // available before
    // container starts
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
    // Setup ScalarDB schema
    configFilePath = tempDir.resolve("scalardb.properties");
    FileUtils.writeStringToFile(configFilePath.toFile(), getScalarDbConfig(), "UTF-8");
  }

  private static String getScalarDbConfig() {
    return "scalar.db.storage=jdbc\n"
        + "scalar.db.contact_points="
        + postgres.getJdbcUrl()
        + "\n"
        + "scalar.db.username="
        + postgres.getUsername()
        + "\n"
        + "scalar.db.password="
        + postgres.getPassword()
        + "\n"
        + "scalar.db.jdbc.driver_class=org.postgresql.Driver\n"
        + "scalar.db.namespace=test\n"
        + "scalar.db.cross_partition_scan.enabled=true\n";
  }

  @Test
  void testImportFromFileWithImportModeUpsertAndFileFormatJsonWithAllColumns() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.json"))
                .toURI());
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
      "JSON",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--require-all-columns"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void testImportFromFileWithImportModeUpsertAndFileFormatJsonWithPrettyPrints() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.json"))
                .toURI());
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
      "JSON",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--pretty-print"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void testImportFromFileAndFileFormatJsonWithIgnoreNulls() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_data_with_null.json"))
                .toURI());
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
      "JSON",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--ignore-nulls"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void testImportFromFileWithImportModeUpsertAndFileFormatJson() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.json"))
                .toURI());
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "JSON",
      "--import-mode", "UPSERT",
      "--file", filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void testImportFromFileWithImportModeUpdateAndFileFormatJson() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.json"))
                .toURI());
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "JSON",
      "--import-mode", "UPDATE",
      "--file", filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void
      testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeTransactionAndFileFormatJSON()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_trn_full.json"))
                .toURI());
    Path controlFilePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("control_files/control_file_trn.json"))
                .toURI());
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "employee_trn",
      "--log-dir",
      tempDir.toString(),
      "--format",
      "JSON",
      "--mode",
      "TRANSACTION",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--control-file",
      controlFilePath.toString(),
      "--control-file-validation",
      "FULL"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void
      testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeAndFileFormatJSONWithTwoTables()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_multi_mapped.json"))
                .toURI());
    Path controlFilePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("control_files/control_file_multi.json"))
                .toURI());
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
      "JSON",
      "--import-mode",
      "UPSERT",
      "--file",
      filePath.toString(),
      "--control-file",
      controlFilePath.toString(),
      "--control-file-validation",
      "FULL",
      "--log-success"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void testImportFromFileWithTransactionModeAndDataChunkSizeAndTransactionSizeWithFormatJson()
      throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_mapped.json"))
                .toURI());
    Path controlFilePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("control_files/control_file_trn_mapped.json"))
                .toURI());
    Path logPath =
        Paths.get(Objects.requireNonNull(getClass().getClassLoader().getResource("logs")).toURI());
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "employee_trn",
      "--log-dir",
      logPath.toString(),
      "--format",
      "JSON",
      "--import-mode",
      "UPSERT",
      "--mode",
      "TRANSACTION",
      "--file",
      filePath.toString(),
      "--control-file",
      controlFilePath.toString(),
      "--control-file-validation",
      "MAPPED",
      "--log-raw-record",
      "--log-success",
      "--transaction-size",
      "1",
      "--data-chunk-size",
      "5"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
  }
}
