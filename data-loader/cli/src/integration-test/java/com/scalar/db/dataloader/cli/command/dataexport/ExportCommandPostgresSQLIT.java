package com.scalar.db.dataloader.cli.command.dataexport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExportCommandPostgresSQLIT {

  private static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:15")
          .withDatabaseName("scalardb")
          .withUsername("postgres")
          .withPassword("12345678")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("init_postgres.sql"),
              "/docker-entrypoint-initdb.d/init_postgres.sql");

  private static final String[] BASE_ARGS = {
    "--namespace", "test", "--table", "all_columns", "--max-threads", "4"
  };

  private static final String[] PROJECTION_ARGS = {
    "--projection", "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11"
  };

  private Path configFilePath;
  @TempDir Path tempDir;

  @BeforeAll
  static void startContainers() {
    postgres.start();
  }

  @AfterAll
  static void stopContainers() {
    postgres.stop();
  }

  @BeforeEach
  void setup() throws IOException {
    configFilePath = tempDir.resolve("scalardb.properties");
    FileUtils.writeStringToFile(configFilePath.toFile(), getScalarDbConfig(), "UTF-8");
  }

  @AfterEach
  void cleanup() throws IOException {
    try (Stream<Path> paths = Files.walk(tempDir)) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException ignored) {
                }
              });
    }
  }

  @Test
  void testCSVBasicExport() throws IOException {
    runAndAssertExport("CSV", PROJECTION_ARGS);
  }

  @Test
  void testJSONWithPrettyPrint() throws IOException {
    runAndAssertExport("JSON", concat("--pretty-print"));
  }

  @Test
  void testJSONLWithPartitionKeyAndFileName() throws IOException {
    runAndAssertExport(
        "JSONL", concat("--partition-key", "col1=1", "--output-file", "sample.jsonl"));
    List<Path> files = findFilesWithExtension(tempDir, ".jsonl");
    assertThat(files.get(0).getFileName().toString()).isEqualTo("sample.jsonl");
  }

  @Test
  void testJSONLWithMetadataAndChunkSizeAndTransaction() throws IOException {
    runAndAssertExport(
        "JSONL", concat("--include-metadata", "--data-chunk-size", "2", "--mode", "TRANSACTION"));
  }

  @Test
  void testCSVWithPartitionKeyDelimiterNoHeaderAndTransaction() throws IOException {
    runAndAssertExport(
        "CSV",
        concat(
            "--partition-key",
            "col1=1",
            "--delimiter",
            ";",
            "--no-header",
            "--mode",
            "TRANSACTION"));
  }

  @Test
  void testBasicCSVExport() throws IOException {
    runAndAssertExport("CSV", PROJECTION_ARGS);
  }

  @Test
  void testBasicJSONExportWithPrettyPrint() throws IOException {
    runAndAssertExport("JSON", concat("--pretty-print"));
  }

  @Test
  void testJSONLWithTransactionAndMetadata() throws IOException {
    runAndAssertExport("JSONL", concat("--include-metadata", "--mode", "TRANSACTION"));
  }

  @Test
  void testCSVWithPartitionKeyAndDelimiter() throws IOException {
    runAndAssertExport("CSV", concat("--partition-key", "col1=1", "--delimiter", ";"));
  }

  @Test
  void testJSONLWithLimitAndChunkSize() throws IOException {
    runAndAssertExport("JSONL", concat("--limit", "2", "--data-chunk-size", "2"));
  }

  @Test
  void testJSONLWithStartEndKeyInclusiveAndTransaction() throws IOException {
    runAndAssertExport(
        "JSONL",
        concat(
            "--start-key",
            "col2=1",
            "--start-inclusive",
            "--end-key",
            "col2=5",
            "--end-inclusive",
            "--mode",
            "TRANSACTION"));
  }

  @Test
  void testJSONLOutputFileName() throws IOException {
    runAndAssertExport("JSONL", concat("--output-file", "sample.jsonl"));
    List<Path> files = findFilesWithExtension(tempDir, ".jsonl");
    assertThat(files.get(0).getFileName().toString()).isEqualTo("sample.jsonl");
  }

  private void runAndAssertExport(String format, String... extraArgs) throws IOException {
    String outputDir = tempDir.toString();
    List<String> args = new ArrayList<>();
    args.add("--config");
    args.add(configFilePath.toString());
    args.add("--output-dir");
    args.add(outputDir);
    args.add("--format");
    args.add(format);
    args.addAll(Arrays.asList(BASE_ARGS));
    args.addAll(Arrays.asList(extraArgs));

    ExportCommand exportCommand = new ExportCommand();
    int exitCode = new CommandLine(exportCommand).execute(args.toArray(new String[0]));
    assertEquals(0, exitCode);

    List<Path> files = findFilesWithExtension(tempDir, formatExtension(format));
    assertThat(files).hasSize(1);
    assertThat(files.get(0).getFileName().toString()).endsWith(formatExtension(format));
  }

  private String formatExtension(String format) {
    String upper = format.toUpperCase();
    switch (upper) {
      case "CSV":
        return ".csv";
      case "JSON":
        return ".json";
      case "JSONL":
        return ".jsonl";
      default:
        throw new IllegalArgumentException("Unknown format: " + format);
    }
  }

  private String[] concat(String... extra) {
    List<String> result = new ArrayList<>();
    result.addAll(Arrays.asList(ExportCommandPostgresSQLIT.PROJECTION_ARGS));
    result.addAll(Arrays.asList(extra));
    return result.toArray(new String[0]);
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

  public static List<Path> findFilesWithExtension(Path dir, String extension) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      return files.filter(path -> path.toString().endsWith(extension)).collect(Collectors.toList());
    }
  }
}
