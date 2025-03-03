package com.scalar.db.dataloader.cli.command.dataexport;

import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.FileFormat;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExportCommandMySQLIT {

    private static final MySQLContainer<?> mysql =
            new MySQLContainer<>("mysql:8.0").withDatabaseName("test_db").withUsername("root") // Use root user
                    .withPassword("12345678"); // Root user has no password by default

    private Path configFilePath;

    @TempDir Path tempDir;

    @BeforeAll
    static void startContainers() {
        mysql.withCopyFileToContainer(
                MountableFile.forClasspathResource("init_mysql.sql"),
                "/docker-entrypoint-initdb.d/init_mysql.sql"
        ); // Ensures the SQL file is available before container starts
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

    @Test
    @Order(0) // Run this before export test
    void testDatabaseInitialization() throws Exception {
        try (Connection conn = mysql.createConnection("");
             Statement stmt = conn.createStatement()) {

            // Verify metadata table exists and has data
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM scalardb.metadata;");
            assertTrue(rs.next());
            int metadataCount = rs.getInt(1);
            assertTrue(metadataCount > 0, "Metadata table should have rows");

            // Verify employee table exists and has data
            rs = stmt.executeQuery("SELECT COUNT(*) FROM test.employee;");
            assertTrue(rs.next());
            int employeeCount = rs.getInt(1);
            assertTrue(employeeCount > 0, "Employee table should have rows");
        }
    }

    @Test
    @Order(1)
    void testExportToFileWithRequiredOptionsWithCSVFormat() throws Exception {
        String outputDir = tempDir.toString();

        ExportCommand exportCommand = new ExportCommand();
        exportCommand.configFilePath = configFilePath.toString();
        exportCommand.namespace = "test";
        exportCommand.table = "employee";
        exportCommand.outputDirectory = outputDir;
        exportCommand.outputFormat = FileFormat.CSV;

        exportCommand.projectionColumns = Collections.singletonList("id");
        assertEquals(0, exportCommand.call());
        File[] files = new File(outputDir).listFiles();
        assertNotNull(files);
        assertEquals(2, files.length);
        assertTrue(files[1].getName().endsWith(".csv"));
        files[1].delete();
    }

    @Test
    @Order(2)
    void testExportToFileWithRequiredOptionsWithJSONFormat() throws Exception {
        String outputDir = tempDir.toString();

        ExportCommand exportCommand = new ExportCommand();
        exportCommand.configFilePath = configFilePath.toString();
        exportCommand.namespace = "test";
        exportCommand.table = "employee";
        exportCommand.outputDirectory = outputDir;
        exportCommand.outputFormat = FileFormat.JSON;

        exportCommand.projectionColumns = Collections.singletonList("id");
        assertEquals(0, exportCommand.call());
        File[] files = new File(outputDir).listFiles();
        assertNotNull(files);
        assertEquals(2, files.length);
        assertTrue(files[1].getName().endsWith(".json"));
        files[1].delete();
    }

    @Test
    @Order(3)
    void testExportToFileWithRequiredOptionsWithJSONFLinesFormat() throws Exception {
        String outputDir = tempDir.toString();

        ExportCommand exportCommand = new ExportCommand();
        exportCommand.configFilePath = configFilePath.toString();
        exportCommand.namespace = "test";
        exportCommand.table = "employee";
        exportCommand.outputDirectory = outputDir;
        exportCommand.outputFormat = FileFormat.JSONL;

        exportCommand.projectionColumns = Collections.singletonList("id");
        assertEquals(0, exportCommand.call());
        File[] files = new File(outputDir).listFiles();
        assertNotNull(files);
        assertEquals(2, files.length);
        assertTrue(files[1].getName().endsWith(".jsonl"));
        files[1].delete();
    }

    @Test
    @Order(4)
    void testExportToFileWithRequiredOptionsWithPartitionKeyFilter() throws Exception {
        String outputDir = tempDir.toString();

        ExportCommand exportCommand = new ExportCommand();
        exportCommand.configFilePath = configFilePath.toString();
        exportCommand.namespace = "test";
        exportCommand.table = "employee";
        exportCommand.outputDirectory = outputDir;
        exportCommand.outputFormat = FileFormat.CSV;
        exportCommand.partitionKeyValue = Collections.singletonList(new ColumnKeyValue("id", "1"));

        exportCommand.projectionColumns = Collections.singletonList("id");
        assertEquals(0, exportCommand.call());
        File[] files = new File(outputDir).listFiles();
        assertNotNull(files);
        assertEquals(2, files.length);
        assertTrue(files[1].getName().endsWith(".csv"));
        files[1].delete();
    }

    @Test
    @Order(1)
    void testExportToFile() throws Exception {
        String outputDir = tempDir.toString();
        String[] args = {

                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--output-dir", outputDir,
                "--format", "CSV"
        };

        ExportCommand exportCommand = new ExportCommand();
        CommandLine commandLine = new CommandLine(exportCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);

        // Verify output file
        File[] files = new File(outputDir).listFiles();
        assertNotNull(files);
        assertEquals(2, files.length);
        assertTrue(files[1].getName().endsWith(".csv"));
        files[1].delete();
    }

    private static String getScalarDbConfig() {
        return "scalar.db.storage=jdbc\n"
                + "scalar.db.contact_points=" + mysql.getJdbcUrl() + "\n"
                + "scalar.db.username=root\n"
                + "scalar.db.password=12345678\n"
                + "scalar.db.cross_partition_scan.enabled=true\n";
    }

}

