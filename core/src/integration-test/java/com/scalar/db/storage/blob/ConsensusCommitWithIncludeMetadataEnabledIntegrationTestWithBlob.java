package com.scalar.db.storage.blob;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitWithIncludeMetadataEnabledIntegrationTestWithBlob
    extends ConsensusCommitWithIncludeMetadataEnabledIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitBlobEnv.getProperties(testName);
  }
}
