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

  private Path getResourcePath(String resource) throws Exception {
    return Paths.get(
        Objects.requireNonNull(getClass().getClassLoader().getResource(resource)).toURI());
  }

  private void executeImport(String... args) {
    ImportCommand importCommand = new ImportCommand();
    CommandLine commandLine = new CommandLine(importCommand);
    assertEquals(0, commandLine.execute(args));
  }

  @Test
  void testImportAllColumns() throws Exception {
    executeImport(
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
        getResourcePath("import_data/import_single.json").toString(),
        "--require-all-columns");
  }

  @Test
  void testImportWithPrettyPrint() throws Exception {
    executeImport(
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
        getResourcePath("import_data/import_single.json").toString(),
        "--pretty-print");
  }

  @Test
  void testImportWithIgnoreNulls() throws Exception {
    executeImport(
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
        getResourcePath("import_data/import_data_with_null.json").toString(),
        "--ignore-nulls");
  }

  @Test
  void testImportBasicUpsert() throws Exception {
    executeImport(
        "--config", configFilePath.toString(),
        "--namespace", "test",
        "--table", "employee",
        "--log-dir", tempDir.toString(),
        "--format", "JSON",
        "--import-mode", "UPSERT",
        "--file", getResourcePath("import_data/import_single.json").toString());
  }

  @Test
  void testImportWithUpdateMode() throws Exception {
    executeImport(
        "--config", configFilePath.toString(),
        "--namespace", "test",
        "--table", "employee",
        "--log-dir", tempDir.toString(),
        "--format", "JSON",
        "--import-mode", "UPDATE",
        "--file", getResourcePath("import_data/import_single.json").toString());
  }

  @Test
  void testImportWithControlFileFullValidation() throws Exception {
    executeImport(
        "--config", configFilePath.toString(),
        "--namespace", "test",
        "--table", "employee_trn",
        "--log-dir", tempDir.toString(),
        "--format", "JSON",
        "--mode", "TRANSACTION",
        "--import-mode", "UPSERT",
        "--file", getResourcePath("import_data/import_single_trn_full.json").toString(),
        "--control-file", getResourcePath("control_files/control_file_trn.json").toString(),
        "--control-file-validation", "FULL");
  }

  @Test
  void testImportMultipleTablesWithControlFile() throws Exception {
    executeImport(
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
        getResourcePath("import_data/import_multi_mapped.json").toString(),
        "--control-file",
        getResourcePath("control_files/control_file_multi.json").toString(),
        "--control-file-validation",
        "FULL",
        "--log-success");
  }

  @Test
  void testImportWithChunksAndTransactions() throws Exception {
    Path logPath = getResourcePath("logs");
    executeImport(
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
        getResourcePath("import_data/import_single_mapped.json").toString(),
        "--control-file",
        getResourcePath("control_files/control_file_trn_mapped.json").toString(),
        "--control-file-validation",
        "MAPPED",
        "--log-raw-record",
        "--log-success",
        "--transaction-size",
        "1",
        "--data-chunk-size",
        "5");
  }
}
