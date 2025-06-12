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

public class ImportCommandJsonMySQLIT {

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
        "/docker-entrypoint-initdb.d/init_mysql.sql");
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

  private int executeCommand(String... args) {
    ImportCommand importCommand = new ImportCommand();
    return new CommandLine(importCommand).execute(args);
  }

  private Path getPathFromResource(String resource) throws Exception {
    return Paths.get(
        Objects.requireNonNull(getClass().getClassLoader().getResource(resource)).toURI());
  }

  @Test
  void testImportWithAllColumns() throws Exception {
    int exitCode =
        executeCommand(
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
            getPathFromResource("import_data/import_single.json").toString(),
            "--require-all-columns");
    assertEquals(0, exitCode);
  }

  @Test
  void testImportWithPrettyPrint() throws Exception {
    int exitCode =
        executeCommand(
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
            getPathFromResource("import_data/import_single.json").toString(),
            "--pretty-print");
    assertEquals(0, exitCode);
  }

  @Test
  void testImportWithIgnoreNulls() throws Exception {
    int exitCode =
        executeCommand(
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
            getPathFromResource("import_data/import_data_with_null.json").toString(),
            "--ignore-nulls");
    assertEquals(0, exitCode);
  }

  @Test
  void testBasicImport() throws Exception {
    int exitCode =
        executeCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee",
            "--log-dir", tempDir.toString(),
            "--format", "JSON",
            "--import-mode", "UPSERT",
            "--file", getPathFromResource("import_data/import_single.json").toString());
    assertEquals(0, exitCode);
  }

  @Test
  void testUpdateModeImport() throws Exception {
    int exitCode =
        executeCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee",
            "--log-dir", tempDir.toString(),
            "--format", "JSON",
            "--import-mode", "UPDATE",
            "--file", getPathFromResource("import_data/import_single.json").toString());
    assertEquals(0, exitCode);
  }

  @Test
  void testTransactionModeWithFullValidation() throws Exception {
    int exitCode =
        executeCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee_trn",
            "--log-dir", tempDir.toString(),
            "--format", "JSON",
            "--mode", "TRANSACTION",
            "--import-mode", "UPSERT",
            "--file", getPathFromResource("import_data/import_single_trn_full.json").toString(),
            "--control-file", getPathFromResource("control_files/control_file_trn.json").toString(),
            "--control-file-validation", "FULL");
    assertEquals(0, exitCode);
  }

  @Test
  void testMultiTableMappedControlFile() throws Exception {
    int exitCode =
        executeCommand(
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
            getPathFromResource("import_data/import_multi_mapped.json").toString(),
            "--control-file",
            getPathFromResource("control_files/control_file_multi.json").toString(),
            "--control-file-validation",
            "FULL",
            "--log-success");
    assertEquals(0, exitCode);
  }

  @Test
  void testTransactionModeWithMappedValidationAndChunks() throws Exception {
    int exitCode =
        executeCommand(
            "--config",
            configFilePath.toString(),
            "--namespace",
            "test",
            "--table",
            "employee_trn",
            "--log-dir",
            getPathFromResource("logs").toString(),
            "--format",
            "JSON",
            "--import-mode",
            "UPSERT",
            "--mode",
            "TRANSACTION",
            "--file",
            getPathFromResource("import_data/import_single_mapped.json").toString(),
            "--control-file",
            getPathFromResource("control_files/control_file_trn_mapped.json").toString(),
            "--control-file-validation",
            "MAPPED",
            "--log-raw-record",
            "--log-success",
            "--transaction-size",
            "1",
            "--data-chunk-size",
            "5");
    assertEquals(0, exitCode);
  }
}
