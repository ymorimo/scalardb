package com.scalar.db.storage.blob;

import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase;
import java.util.Properties;

public class TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestWithBlob
    extends TwoPhaseConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitBlobEnv.getProperties(testName);
  }
}
