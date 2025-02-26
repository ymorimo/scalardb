package com.scalar.db.dataloader.cli.command.dataimport;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;

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
}
