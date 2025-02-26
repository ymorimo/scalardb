package com.scalar.db.storage.blob;

import com.scalar.db.transaction.singlecrudoperation.SingleCrudOperationTransactionIntegrationTestBase;
import java.util.Properties;

public class SingleCrudOperationTransactionIntegrationTestWithBlob
    extends SingleCrudOperationTransactionIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return BlobEnv.getProperties(testName);
  }
}
