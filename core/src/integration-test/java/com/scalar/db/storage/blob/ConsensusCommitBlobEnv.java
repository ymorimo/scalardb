package com.scalar.db.storage.blob;

import com.scalar.db.common.ConsensusCommitTestUtils;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestUtils;
import java.util.Properties;

public class ConsensusCommitBlobEnv {
  private ConsensusCommitBlobEnv() {}

  public static Properties getProperties(String testName) {
    Properties properties = BlobEnv.getProperties(testName);

    // Add testName as a coordinator schema suffix
    ConsensusCommitIntegrationTestUtils.addSuffixToCoordinatorNamespace(properties, testName);

    return ConsensusCommitTestUtils.loadConsensusCommitProperties(properties);
  }
}
