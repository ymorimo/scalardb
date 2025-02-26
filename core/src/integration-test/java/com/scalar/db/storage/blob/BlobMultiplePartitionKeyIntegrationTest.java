package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageMultiplePartitionKeyIntegrationTestBase;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Properties;

public class BlobMultiplePartitionKeyIntegrationTest
    extends DistributedStorageMultiplePartitionKeyIntegrationTestBase {
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

  @Override
  protected Column<?> getColumnWithMinValue(String columnName, DataType dataType) {
    if (dataType == DataType.TEXT) {
      return BlobTestUtils.getMinTextValue(columnName);
    }
    return super.getColumnWithMinValue(columnName, dataType);
  }

  @Override
  protected Column<?> getColumnWithMaxValue(String columnName, DataType dataType) {
    if (dataType == DataType.TEXT) {
      return BlobTestUtils.getMaxTextValue(columnName);
    }
    return super.getColumnWithMaxValue(columnName, dataType);
  }
}
