package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageJapaneseIntegrationTestBase;
import java.util.Properties;

public class BlobJapaneseIntegrationTest extends DistributedStorageJapaneseIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return BlobEnv.getProperties(testName);
  }
}
