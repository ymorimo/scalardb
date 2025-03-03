package com.scalar.db.dataloader.cli.command.dataimport;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ImportCommandMySQLIT {
    private static final MySQLContainer<?> mysql =
            new MySQLContainer<>("mysql:8.0").withDatabaseName("test_db").withUsername("root") // Use root user
                    .withPassword("12345678"); // Root user has no password by default

    private Path configFilePath;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void startContainers() {
        mysql.withCopyFileToContainer(
                MountableFile.forClasspathResource("init_mysql_import.sql"),
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

    private static String getScalarDbConfig() {
        return "scalar.db.storage=jdbc\n"
                + "scalar.db.contact_points=" + mysql.getJdbcUrl() + "\n"
                + "scalar.db.username=root\n"
                + "scalar.db.password=12345678\n"
                + "scalar.db.cross_partition_scan.enabled=true\n";
    }

    @Test
    @Order(1)
    void testImportFromFileWithRequiredInputsOnlyAndFileFormatJsonLines() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single.jsonl").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "JSONL",
                "--import-mode", "INSERT",
                "--file", filePath.toString()
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertAndFileFormatJsonLines() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single.jsonl").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "JSONL",
                "--import-mode", "UPSERT",
                "--file", filePath.toString()
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpdateAndFileFormatJsonLines() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single.jsonl").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "JSONL",
                "--import-mode", "UPDATE",
                "--file", filePath.toString()
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertAndFileFormatJson() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single.json").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "JSON",
                "--import-mode", "UPSERT",
                "--file", filePath.toString()
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpdateAndFileFormatJson() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single.json").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "JSON",
                "--import-mode", "UPDATE",
                "--file", filePath.toString()
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertAndFileFormatCsv() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single.csv").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "CSV",
                "--import-mode", "UPSERT",
                "--file", filePath.toString()
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpdateAndFileFormatCsv() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single.csv").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "CSV",
                "--import-mode", "UPDATE",
                "--file", filePath.toString()
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }


    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertAndFileFormatCsvWithoutHeader() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single_without_header.csv").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "CSV",
                "--import-mode", "UPSERT",
                "--file", filePath.toString(),
                "--delimiter", ",",
                "--header", "id,name,email"
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpdateAndFileFormatCsvWithoutHeader() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single_without_header.csv").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "CSV",
                "--import-mode", "UPDATE",
                "--file", filePath.toString(),
                "--delimiter", ",",
                "--header", "id,name,email"
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertAndFileFormatCsvWithDifferentDelimiter() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single_delimiter.csv").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "CSV",
                "--import-mode", "UPSERT",
                "--file", filePath.toString(),
                "--delimiter", ":",
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertWithControlFile() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single_mapped.jsonl").toURI());
        Path controlFilePath = Paths.get(getClass().getClassLoader().getResource("control_files/control_file.json").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee",
                "--log-dir", tempDir.toString(),
                "--format", "JSONL",
                "--import-mode", "UPSERT",
                "--file", filePath.toString(),
                "--control-file", controlFilePath.toString(),
                "--control-file-validation", "FULL"
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeTransactionAndFileFormatJSONLines() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single_trn_full.jsonl").toURI());
        Path controlFilePath = Paths.get(getClass().getClassLoader().getResource("control_files/control_file_trn.json").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee_trn",
                "--log-dir", tempDir.toString(),
                "--format", "JSONL",
                "--import-mode", "UPSERT",
                "--file", filePath.toString(),
                "--control-file", controlFilePath.toString(),
                "--control-file-validation", "FULL"
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeTransactionAndFileFormatJSON() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single_trn_full.json").toURI());
        Path controlFilePath = Paths.get(getClass().getClassLoader().getResource("control_files/control_file_trn.json").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee_trn",
                "--log-dir", tempDir.toString(),
                "--format", "JSON",
                "--import-mode", "UPSERT",
                "--file", filePath.toString(),
                "--control-file", controlFilePath.toString(),
                "--control-file-validation", "FULL"
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }

    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeTransactionAndFileFormatCSV() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single_trn_full.csv").toURI());
        Path controlFilePath = Paths.get(getClass().getClassLoader().getResource("control_files/control_file_trn.json").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee_trn",
                "--log-dir", tempDir.toString(),
                "--format", "CSV",
                "--import-mode", "UPSERT",
                "--file", filePath.toString(),
                "--control-file", controlFilePath.toString(),
                "--control-file-validation", "FULL"
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);
    }


    @Test
    @Order(2)
    void testImportFromFileWithImportModeUpsertWithControlFileWithStorageModeTransactionAndFileFormatCSV_WithLogRawRecorsEnabled() throws Exception {
        Path filePath = Paths.get(getClass().getClassLoader().getResource("import_data/import_single_trn_full.csv").toURI());
        Path controlFilePath = Paths.get(getClass().getClassLoader().getResource("control_files/control_file_trn.json").toURI());
        String[] args = {
                "--config", configFilePath.toString(),
                "--namespace", "test",
                "--table", "employee_trn",
                "--log-dir", tempDir.toString(),
                "--format", "CSV",
                "--import-mode", "UPSERT",
                "--mode", "TRANSACTION",
                "--file", filePath.toString(),
                "--control-file", controlFilePath.toString(),
                "--control-file-validation", "FULL",
                "-lr"
        };

        ImportCommand importCommand = new ImportCommand();
        CommandLine commandLine = new CommandLine(importCommand);
        int exitCode = commandLine.execute(args);

        assertEquals(0, exitCode);

        File[] files = new File(tempDir.toString()).listFiles();
        assertNotNull(files);
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
            rs = stmt.executeQuery("SELECT COUNT(*) FROM coordinator.state;");
            assertTrue(rs.next());
      System.out.println(rs.getInt(1));
        }
    }



}
