package com.scalar.db.storage.blob;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitNullMetadataIntegrationTestBase;
import java.util.Properties;

public class ConsensusCommitNullMetadataIntegrationTestWithBlob
    extends ConsensusCommitNullMetadataIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitBlobEnv.getProperties(testName);
  }
}
