package com.scalar.db.storage.blob;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitAdminRepairIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitAdminRepairIntegrationTestWithBlob
    extends ConsensusCommitAdminRepairIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    return BlobEnv.getProperties(testName);
  }

  @Override
  protected void initialize(String testName) throws Exception {
    super.initialize(testName);
    adminTestUtils = new BlobAdminTestUtils(getProperties(testName));
  }
}
