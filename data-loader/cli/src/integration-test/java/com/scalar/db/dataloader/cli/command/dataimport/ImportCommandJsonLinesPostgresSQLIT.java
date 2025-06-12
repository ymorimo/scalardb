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

public class ImportCommandJsonLinesPostgresSQLIT {

  private static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>("postgres:15")
          .withDatabaseName("scalardb")
          .withUsername("postgres")
          .withPassword("12345678");

  private Path configFilePath;

  @TempDir Path tempDir;

  @BeforeAll
  static void startContainers() {
    POSTGRES.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_postgres_import.sql"),
        "/docker-entrypoint-initdb.d/init_postgres_import.sql");
    POSTGRES.start();
  }

  @AfterAll
  static void stopContainers() {
    if (POSTGRES != null) {
      POSTGRES.stop();
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
        + POSTGRES.getJdbcUrl()
        + "\n"
        + "scalar.db.username="
        + POSTGRES.getUsername()
        + "\n"
        + "scalar.db.password="
        + POSTGRES.getPassword()
        + "\n"
        + "scalar.db.jdbc.driver_class=org.postgresql.Driver\n"
        + "scalar.db.namespace=test\n"
        + "scalar.db.cross_partition_scan.enabled=true\n";
  }

  private Path getResourcePath(String resource) throws Exception {
    return Paths.get(
        Objects.requireNonNull(getClass().getClassLoader().getResource(resource)).toURI());
  }

  private int executeImportCommand(String... args) {
    return new CommandLine(new ImportCommand()).execute(args);
  }

  @Test
  void testUpsertWithControlFileAndTransactionMode() throws Exception {
    int exitCode =
        executeImportCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee_trn",
            "--log-dir", tempDir.toString(),
            "--format", "JSONL",
            "--import-mode", "UPSERT",
            "--mode", "TRANSACTION",
            "--file", getResourcePath("import_data/import_single_trn_full.jsonl").toString(),
            "--control-file", getResourcePath("control_files/control_file_trn.json").toString(),
            "--control-file-validation", "FULL");
    assertEquals(0, exitCode);
  }

  @Test
  void testRequiredInputsOnly() throws Exception {
    int exitCode =
        executeImportCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee",
            "--log-dir", tempDir.toString(),
            "--format", "JSONL",
            "--import-mode", "INSERT",
            "--file", getResourcePath("import_data/import_single.jsonl").toString());
    assertEquals(0, exitCode);
  }

  @Test
  void testUpsertWithControlFile() throws Exception {
    int exitCode =
        executeImportCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee",
            "--log-dir", tempDir.toString(),
            "--format", "JSONL",
            "--import-mode", "UPSERT",
            "--file", getResourcePath("import_data/import_single_mapped.jsonl").toString(),
            "--control-file", getResourcePath("control_files/control_file.json").toString(),
            "--control-file-validation", "FULL");
    assertEquals(0, exitCode);
  }

  @Test
  void testWithMaxThreads() throws Exception {
    int exitCode =
        executeImportCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee",
            "--log-dir", tempDir.toString(),
            "--format", "JSONL",
            "--import-mode", "INSERT",
            "--file", getResourcePath("import_data/import_single.jsonl").toString(),
            "--max-threads", "4");
    assertEquals(0, exitCode);
  }

  @Test
  void testUpdateImport() throws Exception {
    int exitCode =
        executeImportCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee",
            "--log-dir", tempDir.toString(),
            "--format", "JSONL",
            "--import-mode", "UPDATE",
            "--file", getResourcePath("import_data/import_single.jsonl").toString());
    assertEquals(0, exitCode);
  }

  @Test
  void testUpsertImport() throws Exception {
    int exitCode =
        executeImportCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee",
            "--log-dir", tempDir.toString(),
            "--format", "JSONL",
            "--import-mode", "UPSERT",
            "--file", getResourcePath("import_data/import_single.jsonl").toString());
    assertEquals(0, exitCode);
  }

  @Test
  void testMappedControlFileWithLoggingEnabled() throws Exception {
    int exitCode =
        executeImportCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee_trn",
            "--log-dir", getResourcePath("logs").toString(),
            "--format", "JSONL",
            "--import-mode", "UPSERT",
            "--mode", "TRANSACTION",
            "--file", getResourcePath("import_data/import_single_mapped.jsonl").toString(),
            "--control-file",
                getResourcePath("control_files/control_file_trn_mapped.json").toString(),
            "--control-file-validation", "MAPPED",
            "--log-raw-record", "--log-success");
    assertEquals(0, exitCode);
  }

  @Test
  void testMultiTableControlFile() throws Exception {
    int exitCode =
        executeImportCommand(
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
            getResourcePath("import_data/import_multi_mapped.jsonl").toString(),
            "--control-file",
            getResourcePath("control_files/control_file_multi.json").toString(),
            "--control-file-validation",
            "FULL",
            "--log-success");
    assertEquals(0, exitCode);
  }

  @Test
  void testTransactionAndChunkSize() throws Exception {
    int exitCode =
        executeImportCommand(
            "--config", configFilePath.toString(),
            "--namespace", "test",
            "--table", "employee_trn",
            "--log-dir", getResourcePath("logs").toString(),
            "--format", "JSONL",
            "--import-mode", "UPSERT",
            "--mode", "TRANSACTION",
            "--file", getResourcePath("import_data/import_single_mapped.jsonl").toString(),
            "--control-file",
                getResourcePath("control_files/control_file_trn_mapped.json").toString(),
            "--control-file-validation", "MAPPED",
            "--log-raw-record", "--log-success",
            "--transaction-size", "1",
            "--data-chunk-size", "5");
    assertEquals(0, exitCode);
  }
}
