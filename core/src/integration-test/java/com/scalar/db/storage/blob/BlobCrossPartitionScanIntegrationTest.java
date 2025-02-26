package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageCrossPartitionScanIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class BlobCrossPartitionScanIntegrationTest
    extends DistributedStorageCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return BlobEnv.getProperties(testName);
  }

  @Override
  protected int getThreadNum() {
    return 3;
  }

  @Override
  protected boolean isParallelDdlSupported() {
    return false;
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Blob")
  public void scan_WithOrderingForNonPrimaryColumns_ShouldReturnProperResult() {}
}
