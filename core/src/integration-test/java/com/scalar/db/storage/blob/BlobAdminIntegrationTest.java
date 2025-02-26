package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageAdminIntegrationTestBase;
import com.scalar.db.util.AdminTestUtils;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class BlobAdminIntegrationTest extends DistributedStorageAdminIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return BlobEnv.getProperties(testName);
  }

  @Override
  protected boolean isIndexOnBooleanColumnSupported() {
    return false;
  }

  @Override
  protected AdminTestUtils getAdminTestUtils(String testName) {
    return new BlobAdminTestUtils(getProperties(testName));
  }

  @Test
  @Override
  @Disabled("Index-related operations are not supported in Blob")
  public void createIndex_ForAllDataTypesWithExistingData_ShouldCreateIndexesCorrectly() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported in Blob")
  public void createIndex_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported in Blob")
  public void createIndex_ForNonExistingColumn_ShouldThrowIllegalArgumentException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported in Blob")
  public void createIndex_ForAlreadyExistingIndex_ShouldThrowIllegalArgumentException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported in Blob")
  public void createIndex_IfNotExists_ForAlreadyExistingIndex_ShouldNotThrowAnyException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported in Blob")
  public void dropIndex_ForAllDataTypesWithExistingData_ShouldDropIndexCorrectly() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported in Blob")
  public void dropIndex_ForNonExistingTable_ShouldThrowIllegalArgumentException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported in Blob")
  public void dropIndex_ForNonExistingIndex_ShouldThrowIllegalArgumentException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported in Blob")
  public void dropIndex_IfExists_ForNonExistingIndex_ShouldNotThrowAnyException() {}
}
