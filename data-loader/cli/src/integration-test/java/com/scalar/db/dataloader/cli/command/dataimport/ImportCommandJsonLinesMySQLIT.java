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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

public class ImportCommandJsonLinesMySQLIT {
  private static final MySQLContainer<?> mysql =
      new MySQLContainer<>("mysql:8.0")
          .withDatabaseName("test")
          .withUsername("root")
          .withPassword("12345678");

  private Path configFilePath;

  @TempDir Path tempDir;

  @BeforeAll
  static void startContainers() {
    mysql.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_mysql_import.sql"),
        "/docker-entrypoint-initdb.d/init_mysql.sql"); // Ensures the SQL file is available before
    // container starts
    mysql.start();
  }

  @AfterAll
  static void stopContainers() {
    if (mysql != null) {
      mysql.stop();
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
        + mysql.getJdbcUrl()
        + "\n"
        + "scalar.db.username=root\n"
        + "scalar.db.password=12345678\n"
        + "scalar.db.cross_partition_scan.enabled=true\n";
  }

  @Test
  void
      testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeTransactionAndFileFormatJSONLines()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_trn_full.jsonl"))
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
      "JSONL",
      "--import-mode",
      "UPSERT",
      "--mode",
      "TRANSACTION",
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
  void testImportFromFileWithRequiredInputsOnlyAndFileFormatJsonLines() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.jsonl"))
                .toURI());
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "JSONL",
      "--import-mode", "INSERT",
      "--file", filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void testImportFromFileWithImportModeUpsertWithControlFile() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_mapped.jsonl"))
                .toURI());
    Path controlFilePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("control_files/control_file.json"))
                .toURI());
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "JSONL",
      "--import-mode", "UPSERT",
      "--file", filePath.toString(),
      "--control-file", controlFilePath.toString(),
      "--control-file-validation", "FULL"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void testImportFromFileWithRequiredInputsOnlyAndFileFormatJsonLinesWithMaxThreads()
      throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.jsonl"))
                .toURI());
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "JSONL",
      "--import-mode", "INSERT",
      "--file", filePath.toString(),
      "--max-threads", "4"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void testImportFromFileAndFileFormatJsonLines() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.jsonl"))
                .toURI());
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "JSONL",
      "--import-mode", "UPDATE",
      "--file", filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void testImportFromFileWithImportModeUpsertAndFileFormatJsonLines() throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass().getClassLoader().getResource("import_data/import_single.jsonl"))
                .toURI());
    String[] args = {
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "employee",
      "--log-dir", tempDir.toString(),
      "--format", "JSONL",
      "--import-mode", "UPSERT",
      "--file", filePath.toString()
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);

    assertEquals(0, exitCode);
  }

  @Test
  void
      testImportFromFileWithImportModeUpsertWithControlFileMappedWithStorageModeTransactionAndFileFormatCSV_WithLogRawRecordsEnabled()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_mapped.jsonl"))
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
      "JSONL",
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
      "--log-success"
    };

    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
  }

  @Test
  void
      testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeAndFileFormatJSONLinesWithTwoTables()
          throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_multi_mapped.jsonl"))
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
      "JSONL",
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
  void testImportFromFileWithTransactionModeAndDataChunkSizeAndTransactionSizeWithFormatJSONLines()
      throws Exception {
    Path filePath =
        Paths.get(
            Objects.requireNonNull(
                    getClass()
                        .getClassLoader()
                        .getResource("import_data/import_single_mapped.jsonl"))
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
      "JSONL",
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
