// package com.scalar.db.dataloader.cli.command.dataexport;
//
// import static org.junit.jupiter.api.Assertions.assertEquals;
// import static org.junit.jupiter.api.Assertions.assertNotNull;
// import static org.junit.jupiter.api.Assertions.assertTrue;
//
// import com.scalar.db.api.DistributedStorage;
// import com.scalar.db.dataloader.core.FileFormat;
// import com.scalar.db.service.StorageFactory;
// import java.io.File;
// import java.io.IOException;
// import java.nio.file.Path;
// import java.sql.Connection;
// import java.sql.ResultSet;
// import java.sql.Statement;
// import java.util.Collections;
// import org.apache.commons.io.FileUtils;
// import org.junit.jupiter.api.AfterAll;
// import org.junit.jupiter.api.BeforeAll;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.MethodOrderer;
// import org.junit.jupiter.api.Order;
// import org.junit.jupiter.api.Test;
// import org.junit.jupiter.api.TestInstance;
// import org.junit.jupiter.api.TestMethodOrder;
// import org.junit.jupiter.api.io.TempDir;
// import org.testcontainers.containers.PostgreSQLContainer;
// import org.testcontainers.utility.MountableFile;
//
// @TestInstance(TestInstance.Lifecycle.PER_CLASS)
// @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
// public class ExportCommandPostgresIT {
//
//  private static final PostgreSQLContainer<?> postgres =
//      new PostgreSQLContainer<>("postgres:15")
//          .withDatabaseName("test_db")
//          .withUsername("postgres")
//          .withPassword("12345678");
//
//  private DistributedStorage storage;
//  private Path configFilePath;
//
//  @TempDir Path tempDir;
//
//  @BeforeAll
//  static void startContainers() {
//    postgres.withCopyFileToContainer(
//        MountableFile.forClasspathResource("init_postgres.sql"),
//        "/docker-entrypoint-initdb.d/init_postgres.sql");
//    postgres.start();
//  }
//
//  @AfterAll
//  static void stopContainers() {
//    postgres.stop();
//  }
//
//  @BeforeEach
//  void setup() throws Exception {
//    // Setup ScalarDB schema
//    configFilePath = tempDir.resolve("scalardb.properties");
//    FileUtils.writeStringToFile(configFilePath.toFile(), getScalarDbConfig(), "UTF-8");
//
//    StorageFactory factory = StorageFactory.create(configFilePath.toString());
//    storage = factory.getStorage();
//  }
//
//  @Test
//  @Order(0)
//  void testDatabaseInitialization() throws Exception {
//    try (Connection conn = postgres.createConnection("");
//        Statement stmt = conn.createStatement()) {
//
//      // Verify metadata table exists and has data
//      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM scalardb.metadata;");
//      assertTrue(rs.next());
//      int metadataCount = rs.getInt(1);
//      assertTrue(metadataCount > 0, "Metadata table should have rows");
//
//      // Verify employee table exists and has data
//      rs = stmt.executeQuery("SELECT COUNT(*) FROM test.employee;");
//      assertTrue(rs.next());
//      int employeeCount = rs.getInt(1);
//      assertTrue(employeeCount > 0, "Employee table should have rows");
//    }
//  }
//
//  @Test
//  @Order(1)
//  void testExportToFileWithRequiredOptions() throws Exception {
//    String outputDir = tempDir.toString();
//
//    ExportCommand exportCommand = new ExportCommand();
//    exportCommand.configFilePath = configFilePath.toString();
//    exportCommand.namespace = "test";
//    exportCommand.table = "employee";
//    exportCommand.outputDirectory = outputDir;
//    exportCommand.outputFormat = FileFormat.CSV;
//
//    exportCommand.projectionColumns = Collections.singletonList("id");
//    assertEquals(0, exportCommand.call());
//    File[] files = new File(outputDir).listFiles();
//    assertNotNull(files);
//    assertEquals(2, files.length);
//    assertTrue(files[1].getName().endsWith(".csv"));
//    files[1].delete();
//  }
//
//  private static String getScalarDbConfig() {
//    return "scalar.db.storage=jdbc\n"
//        + "scalar.db.contact_points="
//        + postgres.getJdbcUrl()
//        + "\n"
//        + "scalar.db.username=root\n"
//        + "scalar.db.password=12345678\n"
//        + "scalar.db.cross_partition_scan.enabled=true\n";
//  }
//
//  @AfterAll
//  void tearDown() throws IOException {
//    if (postgres != null) {
//      postgres.stop();
//    }
//  }
// }
