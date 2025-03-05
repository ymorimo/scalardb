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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExportCommandMySQLIT {

  private static final MySQLContainer<?> mysql =
      new MySQLContainer<>("mysql:8.0")
          .withDatabaseName("test")
          .withUsername("root") // Use root user
          .withPassword("12345678"); // Root user has no password by default
  private static final String CSV_EXTENSION = ".csv";
  private static final String JSON_EXTENSION = ".json";
  private static final String JSONLINES_EXTENSION = ".jsonl";

  private Path configFilePath;

  @TempDir Path tempDir;

  @BeforeAll
  static void startContainers() {
    mysql.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_mysql.sql"),
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "CSV"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "JSON",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "JSON",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--pretty-print"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "CSV",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "JSON",
      "--partition-key", "col1=1",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "JSONL",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--output-file", "sample.jsonl",
      "--format", "JSONL",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "JSONL",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--limit", "2"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "JSONL",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--start-key", "col2=1",
      "--end-key", "col2=5"
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
      "--end-inclusive"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "JSONL",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--include-metadata"
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
      "2"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "CSV",
      "--partition-key", "col1=1",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "CSV",
      "--partition-key", "col1=1",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--delimiter", ";"
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
      "--config", configFilePath.toString(),
      "--namespace", "test",
      "--table", "all_columns",
      "--output-dir", outputDir,
      "--format", "CSV",
      "--partition-key", "col1=1",
      "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11",
      "--delimiter", ";",
      "--no-header"
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
      "2"
    };

    ExportCommand exportCommand = new ExportCommand();
    CommandLine commandLine = new CommandLine(exportCommand);
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);
    List<Path> files = findFilesWithExtension(tempDir, JSONLINES_EXTENSION);
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(JSONLINES_EXTENSION);
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

  public static List<Path> findFilesWithExtension(Path directory, String extension)
      throws IOException {
    try (Stream<Path> files = Files.list(directory)) {
      return files.filter(path -> path.toString().endsWith(extension)).collect(Collectors.toList());
    }
  }
}
