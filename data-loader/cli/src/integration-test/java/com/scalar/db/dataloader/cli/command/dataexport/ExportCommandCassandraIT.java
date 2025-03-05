// package com.scalar.db.dataloader.cli.command.dataexport;
//
// import static org.junit.jupiter.api.Assertions.*;
//
// import com.scalar.db.api.DistributedStorage;
// import com.scalar.db.dataloader.core.FileFormat;
// import com.scalar.db.service.StorageFactory;
// import java.io.File;
// import java.io.IOException;
// import java.nio.file.Path;
// import java.util.Collections;
// import org.apache.commons.io.FileUtils;
// import org.junit.jupiter.api.*;
// import org.junit.jupiter.api.io.TempDir;
// import org.testcontainers.containers.CassandraContainer;
//
// @TestInstance(TestInstance.Lifecycle.PER_CLASS)
// @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
// public class ExportCommandCassandraIT {
//
//  private static final CassandraContainer<?> cassandra =
//      new CassandraContainer<>("cassandra:3.11.14").withInitScript("init-cassandra.cql");
//
//  private DistributedStorage storage;
//  private Path configFilePath;
//
//  @TempDir Path tempDir;
//
//  @BeforeAll
//  static void startContainers() {
//    cassandra.start();
//    //        importCqlScript();
//  }
//
//  //    private static void importCqlScript() {
//  //        try (CqlSession session = CqlSession.builder()
//  //                .addContactPoint(cassandra.getContactPoint())
//  //                .withLocalDatacenter("datacenter1")
//  //                .build()) {
//  //            Path initCql = Path.of("src/test/resources/init.cql");
//  //            List<String> queries = Files.readAllLines(initCql);
//  //            for (String query : queries) {
//  //                if (!query.trim().isEmpty()) {
//  //                    session.execute(query);
//  //                }
//  //            }
//  //        } catch (IOException e) {
//  //            throw new RuntimeException("Failed to import CQL script", e);
//  //        }
//  //    }
//
//  @AfterAll
//  static void stopContainers() {
//    cassandra.stop();
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
//  void testDatabaseInitialization() {
//    // Verify that the storage is initialized (Cassandra tables checked via ScalarDB)
//    assertNotNull(storage);
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
//    exportCommand.projectionColumns = Collections.singletonList("id");
//    assertEquals(0, exportCommand.call());
//
//    File[] files = new File(outputDir).listFiles();
//    assertNotNull(files);
//    assertEquals(2, files.length);
//    assertTrue(files[1].getName().endsWith(".csv"));
//    files[1].delete();
//  }
//
//  @AfterAll
//  void tearDown() throws IOException {
//    if (cassandra != null) {
//      cassandra.stop();
//    }
//  }
//
//  private static String getScalarDbConfig() {
//    return "scalar.db.storage=cassandra\n"
//        + "scalar.db.contact_points="
//        + cassandra.getContactPoint().getAddress().getHostAddress()
//        + "\n"
//        + "scalar.db.contact_port="
//        + cassandra.getMappedPort(9042)
//        + "\n"
//        + "scalar.db.username=cassandra\n"
//        + "scalar.db.password=cassandra\n"
//        + "scalar.db.consistency_level=QUORUM\n";
//  }
// }
