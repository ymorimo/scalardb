package com.scalar.db.storage.blob;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitCrossPartitionScanIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitCrossPartitionScanIntegrationTestWithBlob
    extends ConsensusCommitCrossPartitionScanIntegrationTestBase {

  @Override
  protected Properties getProps(String testName) {
    Properties properties = ConsensusCommitBlobEnv.getProperties(testName);
    properties.setProperty(ConsensusCommitConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    return properties;
  }

  @Test
  @Override
  @Disabled("Cross partition scan with ordering is not supported in Blob")
  public void scan_CrossPartitionScanWithOrderingGivenForCommittedRecord_ShouldReturnRecords() {}
}
