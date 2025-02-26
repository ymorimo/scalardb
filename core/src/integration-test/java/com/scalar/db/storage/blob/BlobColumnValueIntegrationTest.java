package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageColumnValueIntegrationTestBase;
import java.util.Properties;

public class BlobColumnValueIntegrationTest
    extends DistributedStorageColumnValueIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return BlobEnv.getProperties(testName);
  }
}
