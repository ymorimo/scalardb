package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageAdminRepairIntegrationTestBase;
import java.util.Properties;

public class BlobAdminRepairIntegrationTest
    extends DistributedStorageAdminRepairIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return BlobEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    adminTestUtils = new BlobAdminTestUtils(getProperties(testName));
  }
}
