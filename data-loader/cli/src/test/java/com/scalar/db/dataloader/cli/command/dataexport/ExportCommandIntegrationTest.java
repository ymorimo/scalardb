package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.*;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.service.StorageFactory;
import lombok.var;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExportCommandIntegrationTest {

    private static final MySQLContainer<?> mysql =
            new MySQLContainer<>("mysql:8.0").withDatabaseName("test_db").withUsername("root") // Use root user
                    .withPassword("12345678"); // Root user has no password by default

    private DistributedStorage storage;
    private Path configFilePath;

    @TempDir Path tempDir;

    @BeforeAll
    static void startContainers() {
        mysql.withCopyFileToContainer(
                MountableFile.forClasspathResource("init.sql"),
                "/docker-entrypoint-initdb.d/init.sql"
        ); // Ensures the SQL file is available before container starts
        mysql.start();
    }

    @AfterAll
    static void stopContainers() {
        mysql.stop();
    }

    @BeforeEach
    void setup() throws Exception {
        // Setup ScalarDB schema
        configFilePath = tempDir.resolve("scalardb.properties");
        FileUtils.writeStringToFile(configFilePath.toFile(), getScalarDbConfig(), "UTF-8");

        StorageFactory factory = StorageFactory.create(configFilePath.toString());
        storage = factory.getStorage();

    }

    @Test
    @Order(0) // Run this before export test
    void testDatabaseInitialization() throws Exception {
        try (var conn = mysql.createConnection("");
             var stmt = conn.createStatement()) {

            // Verify metadata table exists and has data
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM scalardb.metadata;");
            assertTrue(rs.next());
            int metadataCount = rs.getInt(1);
            assertTrue(metadataCount > 0, "Metadata table should have rows");

            // Verify employee table exists and has data
            rs = stmt.executeQuery("SELECT COUNT(*) FROM TEST.employee;");
            assertTrue(rs.next());
            int employeeCount = rs.getInt(1);
            assertTrue(employeeCount > 0, "Employee table should have rows");
        }
    }

    @Test
    @Order(1)
    void testExportToFile() throws Exception {
        String outputDir = tempDir.toString();
        String[] args = {
                "export",
                "-c", configFilePath.toString(),
                "-ns", "test",
                "-t", "employee",
                "-d", outputDir,
                "-fmt", "CSV",
                "-pk", "id=1",
                "-p", "id,name,email"

        };

        ExportCommand exportCommand = new ExportCommand();
        exportCommand.configFilePath = configFilePath.toString();
        exportCommand.namespace = "test";
        exportCommand.table = "employee";
        exportCommand.outputDirectory = outputDir;
        exportCommand.outputFormat = FileFormat.CSV;
        exportCommand.partitionKeyValue = Collections.singletonList(new ColumnKeyValue("id", "1"));

        exportCommand.projectionColumns = Collections.singletonList("id");
        assertEquals(0, exportCommand.call());
//        CommandLine commandLine = new CommandLine(exportCommand);
//        int exitCode = commandLine.execute(args);
//        ExportCommand exportCommand = new ExportCommand();
//        CommandLine commandLine = new CommandLine(exportCommand);
//        int exitCode = commandLine.execute(args);

//        assertEquals(0, exitCode);
//         Verify output file
        File[] files = new File(outputDir).listFiles();
        assertNotNull(files);
    System.out.println(files.length);
    if(files.length ==2){
        assertTrue(files[1].getName().endsWith(".csv"));
    }
    }

    private static String getScalarDbConfig() {
        return "scalar.db.storage=jdbc\n"
                + "scalar.db.contact_points=" + mysql.getJdbcUrl() + "\n"
                + "scalar.db.username=root\n"
                + "scalar.db.password=12345678\n";
    }

    @AfterAll
    void tearDown() throws IOException {
        // Stop PostgreSQL container
        if (mysql != null) {
            mysql.stop();
        }

//        // Delete temp directory
//        Files.walk(tempDir)
//                .sorted(Comparator.reverseOrder())
//                .forEach(p -> {
//                    try { Files.delete(p); } catch (IOException ignored) {}
//                });
    }

}

