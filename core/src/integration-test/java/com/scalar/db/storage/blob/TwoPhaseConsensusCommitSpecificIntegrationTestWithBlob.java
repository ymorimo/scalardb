package com.scalar.db.storage.blob;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseConsensusCommitSpecificIntegrationTestWithBlob
    extends TwoPhaseConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties1(String testName) {
    return ConsensusCommitBlobEnv.getProperties(testName);
  }
}
