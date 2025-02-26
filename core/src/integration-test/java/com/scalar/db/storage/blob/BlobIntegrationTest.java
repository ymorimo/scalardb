package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class BlobIntegrationTest extends DistributedStorageIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return BlobEnv.getProperties(testName);
  }

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void get_GetGivenForIndexedColumn_ShouldGet() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void get_GetGivenForIndexedColumnWithMatchedConjunctions_ShouldGet() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void get_GetGivenForIndexedColumnWithUnmatchedConjunctions_ShouldReturnEmpty() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void
      get_GetGivenForIndexedColumnMatchingMultipleRecords_ShouldThrowIllegalArgumentException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void scan_ScanGivenForIndexedColumn_ShouldScan() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void scan_ScanGivenForNonIndexedColumn_ShouldThrowIllegalArgumentException() {}
}
