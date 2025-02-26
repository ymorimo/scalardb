package com.scalar.db.storage.blob;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestWithBlob
    extends TwoPhaseConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps1(String testName) {
    Properties properties = ConsensusCommitBlobEnv.getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    return properties;
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Blob")
  public void scan_ScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords() {}
}
