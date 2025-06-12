package com.scalar.db.dataloader.cli.command.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

public class ImportCommandCsvMySQLIT {

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

  private int runImportCommand(String... args) {
    ImportCommand importCommand = new ImportCommand();
    return new CommandLine(importCommand).execute(args);
  }

  private Path getPathFromResource(String resource) throws Exception {
    return Paths.get(getClass().getClassLoader().getResource(resource).toURI());
  }

  private List<String> buildCommonArgs(String file, String table, String importMode)
      throws Exception {
    List<String> args = new ArrayList<>();
    args.add("--config");
    args.add(configFilePath.toString());
    args.add("--namespace");
    args.add("test");
    args.add("--table");
    args.add(table);
    args.add("--log-dir");
    args.add(tempDir.toString());
    args.add("--format");
    args.add("CSV");
    args.add("--import-mode");
    args.add(importMode);
    args.add("--file");
    args.add(getPathFromResource(file).toString());
    return args;
  }

  @Test
  void testImportWithControlFileAndTransactionMode() throws Exception {
    List<String> args =
        buildCommonArgs("import_data/import_single_trn_full.csv", "employee_trn", "UPSERT");
    args.add("--mode");
    args.add("TRANSACTION");
    args.add("--control-file");
    args.add(getPathFromResource("control_files/control_file_trn.json").toString());
    args.add("--control-file-validation");
    args.add("FULL");
    args.add("--log-success");

    assertEquals(0, runImportCommand(args.toArray(new String[0])));
  }

  @Test
  void testImportSimpleUpsert() throws Exception {
    List<String> args = buildCommonArgs("import_data/import_data_all.csv", "all_columns", "UPSERT");
    assertEquals(0, runImportCommand(args.toArray(new String[0])));
  }

  @Test
  void testImportSimpleInsert() throws Exception {
    List<String> args = buildCommonArgs("import_data/import_data_all.csv", "all_columns", "INSERT");
    assertEquals(0, runImportCommand(args.toArray(new String[0])));
  }

  @Test
  void testImportSimpleUpdate() throws Exception {
    List<String> args = buildCommonArgs("import_data/import_data_all.csv", "all_columns", "UPDATE");
    assertEquals(0, runImportCommand(args.toArray(new String[0])));
  }

  @Test
  void testImportWithControlFileMultiTable() throws Exception {
    List<String> args =
        buildCommonArgs("import_data/import_multi_mapped.csv", "employee", "UPSERT");
    args.add("--control-file");
    args.add(getPathFromResource("control_files/control_file_multi.json").toString());
    args.add("--control-file-validation");
    args.add("FULL");
    args.add("--log-success");

    assertEquals(0, runImportCommand(args.toArray(new String[0])));
  }

  @Test
  void testImportWithNoHeader() throws Exception {
    List<String> args =
        buildCommonArgs("import_data/import_single_without_header.csv", "employee", "UPSERT");
    args.add("--delimiter");
    args.add(",");
    args.add("--header");
    args.add("id,name,email");

    assertEquals(0, runImportCommand(args.toArray(new String[0])));
  }

  @Test
  void testImportWithDifferentDelimiter() throws Exception {
    List<String> args =
        buildCommonArgs("import_data/import_single_delimiter.csv", "employee", "UPSERT");
    args.add("--delimiter");
    args.add(":");

    assertEquals(0, runImportCommand(args.toArray(new String[0])));
  }

  @Test
  void testImportWithSplitLogMode() throws Exception {
    List<String> args =
        buildCommonArgs("import_data/import_single_mapped.csv", "employee_trn", "UPSERT");
    args.add("--mode");
    args.add("TRANSACTION");
    args.add("--control-file");
    args.add(getPathFromResource("control_files/control_file_trn_mapped.json").toString());
    args.add("--control-file-validation");
    args.add("MAPPED");
    args.add("--log-raw-record");
    args.add("--log-success");
    args.add("--split-log-mode");

    assertEquals(0, runImportCommand(args.toArray(new String[0])));
  }

  @Test
  void testImportWithChunkAndTxnSize() throws Exception {
    List<String> args =
        buildCommonArgs("import_data/import_single_mapped.csv", "employee_trn", "UPSERT");
    args.add("--mode");
    args.add("TRANSACTION");
    args.add("--control-file");
    args.add(getPathFromResource("control_files/control_file_trn_mapped.json").toString());
    args.add("--control-file-validation");
    args.add("MAPPED");
    args.add("--log-raw-record");
    args.add("--log-success");
    args.add("--transaction-size");
    args.add("1");
    args.add("--data-chunk-size");
    args.add("5");

    assertEquals(0, runImportCommand(args.toArray(new String[0])));
  }
}
