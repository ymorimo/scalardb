package com.scalar.db.dataloader.cli.command.dataimport;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;

//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ImportCommandMySQLTest {

//  private static final String SCALARDB_PROJECT_PATH = "../../.."; // Adjusted to match cli/core test location
//  private static final String SCHEMA_LOADER_PROJECT_PATH = SCALARDB_PROJECT_PATH + "/schema-loader";
//  private static PostgreSQLContainer<?> postgres;
//  private String p = "";
//  private Path tempDir;
//
//  @BeforeAll
//  void setUp() throws IOException, InterruptedException {
//
//    Path currentPath = Paths.get("").toAbsolutePath();
//    System.out.println("Current working directory using Path: " + currentPath);
//
//    String workingDir = System.getProperty("user.dir");
//    System.out.println("Current working directory: " + workingDir);
//    String[] path = workingDir.split("data-loader");
//    System.out.println(path[0]);
//    System.out.println(path[1]);
//
//    // Start PostgreSQL container
//    postgres = new PostgreSQLContainer<>("postgres:latest");
//    postgres.start();
//
//    // Create a temp directory for schema files
//    tempDir = Files.createTempDirectory("scalardb-schema");
//    p = path[0] + "schema-loader";
//    // Build ScalarDB Schema Loader JAR using Gradle from main scalardb folder
//    ProcessBuilder processBuilder = new ProcessBuilder(
//            path[0] + "./gradlew", ":schema-loader:build"
//    );
//    Process process = processBuilder.start();
//    int exitCode = process.waitFor();
//    assertEquals(0, exitCode, "Failed to build ScalarDB Schema Loader");
//  }
//
//  @Test
//  void testSchemaLoader() throws IOException, InterruptedException {
//    // Define the generated JAR path
//    Path jarPath = Paths.get(p, "build", "libs", "scalardb-schema-loader-4.0.0-SNAPSHOT.jar");
//    assertTrue(Files.exists(jarPath), "Schema Loader JAR is missing");
//
//    // Generate a schema file
//    Path schemaFile = tempDir.resolve("schema.json");
//    Files.write(schemaFile, "{\"database\":\"test\"}".getBytes());
//
//    // Run the schema loader
//    ProcessBuilder loaderProcess = new ProcessBuilder(
//            "java", "-jar", jarPath.toString(), "--config", schemaFile.toString(),
//            "--host", postgres.getHost(), "--port", postgres.getFirstMappedPort().toString(),
//            "--user", postgres.getUsername(), "--password", postgres.getPassword()
//    );
//    Process process = loaderProcess.start();
//    int exitCode = process.waitFor();
//    assertEquals(0, exitCode, "Schema Loader execution failed");
//  }
//
//  @AfterAll
//  void tearDown() throws IOException {
//    // Stop PostgreSQL container
//    if (postgres != null) {
//      postgres.stop();
//    }
//
//    // Delete temp directory
//    Files.walk(tempDir)
//            .sorted(Comparator.reverseOrder())
//            .forEach(p -> {
//              try { Files.delete(p); } catch (IOException ignored) {}
//            });
//  }
}
