package com.scalar.db.storage.blob;

import com.scalar.db.api.DistributedStorageSinglePartitionKeyIntegrationTestBase;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.Properties;

public class BlobSinglePartitionKeyIntegrationTest
    extends DistributedStorageSinglePartitionKeyIntegrationTestBase {
  @Override
  protected Properties getProperties(String testName) {
    return BlobEnv.getProperties(testName);
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
