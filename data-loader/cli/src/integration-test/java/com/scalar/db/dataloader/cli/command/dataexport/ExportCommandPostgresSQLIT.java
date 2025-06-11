package com.scalar.db.dataloader.cli.command.dataexport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExportCommandPostgresSQLIT {

  private static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:15")
          .withDatabaseName("scalardb")
          .withUsername("postgres")
          .withPassword("12345678");
  private static final String CSV_EXTENSION = ".csv";
  private static final String JSON_EXTENSION = ".json";
  private static final String JSONLINES_EXTENSION = ".jsonl";

  private Path configFilePath;

  @TempDir Path tempDir;

  @BeforeAll
  static void startContainers() {
    postgres.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_postgres.sql"),
        "/docker-entrypoint-initdb.d/init_postgres.sql"); // Ensures the SQL file is available
    // before
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

  @AfterEach
  void removeFiles() throws IOException {
    try (Stream<Path> paths = Files.walk(tempDir)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  System.err.println("Failed to delete file: " + path);
                }
              });
    }
  }

  @Test
  void testExportToFile() throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);

    // Verify output file
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFormat() throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSON",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSON_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSON_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFormatWithPrettyPrint() throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSON",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--pretty-print",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSON_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSON_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithCSVFormatWithPartitionKeyFilter() throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFormatWithPartitionKeyFilter()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSON",
      "--partition-key",
      "col1=1",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSON_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSON_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONLinesFormat() throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONLinesFormatWithFileName() throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--output-file",
      "sample.jsonl",
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--max-threads",
      "4"
    };
    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
    assertThat(files.get(0).getFileName().toString()).isEqualTo("sample.jsonl");
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithLimit() throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--limit",
      "2",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithScanStartAndEnd()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--start-key",
      "col2=1",
      "--end-key",
      "col2=5",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithScanStartAndEndAndInclusive()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--start-key",
      "col2=1",
      "--start-inclusive",
      "--end-key",
      "col2=5",
      "--end-inclusive",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONLinesFormatWithMetadata() throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--include-metadata",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithMetadataWithMaxThread()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--include-metadata",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithPartitionKeyFilter() throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--partition-key",
      "col1=1",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithPartitionKeyFilterWithDelimiter() throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--partition-key",
      "col1=1",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--delimiter",
      ";",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithPartitionKeyFilterWithDelimiterAndNoHeader()
      throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--partition-key",
      "col1=1",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--delimiter",
      ";",
      "--no-header",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithMetadataWithDataChunkSize()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--include-metadata",
      "--data-chunk-size",
      "2",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithTransaction() throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);

    // Verify output file
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFormatWithTransaction() throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSON",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSON_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSON_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFormatWithPrettyPrintWithTransaction()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSON",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--pretty-print",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSON_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSON_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithCSVFormatWithPartitionKeyFilterWithTransaction()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFormatWithPartitionKeyFilterWithTransaction()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSON",
      "--partition-key",
      "col1=1",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSON_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSON_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONLinesFormatWithTransaction() throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONLinesFormatWithFileNameWithTransaction()
      throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--output-file",
      "sample.jsonl",
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };
    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
    assertThat(files.get(0).getFileName().toString()).isEqualTo("sample.jsonl");
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithLimitWithTransaction()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--limit",
      "2",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithScanStartAndEndWithTransaction()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--start-key",
      "col2=1",
      "--end-key",
      "col2=5",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void
      testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithScanStartAndEndAndInclusiveWithTransaction()
          throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--start-key",
      "col2=1",
      "--start-inclusive",
      "--end-key",
      "col2=5",
      "--end-inclusive",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithJSONLinesFormatWithMetadataWithTransaction()
      throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--include-metadata",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void
      testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithMetadataWithMaxThreadWithTransaction()
          throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--include-metadata",
      "--max-threads",
      "4",
      "--mode",
      "TRANSACTION"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithPartitionKeyFilterWithTransaction()
      throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--partition-key",
      "col1=1",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void testExportToFileWithRequiredOptionsWithPartitionKeyFilterWithDelimiterWithTransaction()
      throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--partition-key",
      "col1=1",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--delimiter",
      ";",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void
      testExportToFileWithRequiredOptionsWithPartitionKeyFilterWithDelimiterAndNoHeaderWithTransaction()
          throws IOException {
    String outputDir = tempDir.toString();
    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "CSV",
      "--partition-key",
      "col1=1",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--delimiter",
      ";",
      "--no-header",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, CSV_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(CSV_EXTENSION);
  }

  @Test
  void
      testExportToFileWithRequiredOptionsWithJSONFLinesFormatWithMetadataWithDataChunkSizeWithTransaction()
          throws IOException {
    String outputDir = tempDir.toString();

    String[] args = {
      "--config",
      configFilePath.toString(),
      "--namespace",
      "test",
      "--table",
      "all_columns",
      "--output-dir",
      outputDir,
      "--format",
      "JSONL",
      "--projection",
      "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--include-metadata",
      "--data-chunk-size",
      "2",
      "--mode",
      "TRANSACTION",
      "--max-threads",
      "4"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
  }

  public static List<Path> findFilesWithExtension(Path directory, String extension)
      throws IOException {
    try (Stream<Path> files = Files.list(directory)) {
      return files.filter(path -> path.toString().endsWith(extension)).collect(Collectors.toList());
    }
  }
}
