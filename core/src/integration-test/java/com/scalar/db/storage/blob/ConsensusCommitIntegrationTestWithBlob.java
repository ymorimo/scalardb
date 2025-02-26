package com.scalar.db.storage.blob;

import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitIntegrationTestWithBlob extends ConsensusCommitIntegrationTestBase {
  @Override
  protected Properties getProps(String testName) {
    return ConsensusCommitBlobEnv.getProperties(testName);
  }

  @Override
  protected boolean isTimestampTypeSupported() {
    return false;
  }

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void get_GetGivenForIndexColumn_ShouldReturnRecords() throws TransactionException {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void scan_ScanGivenForIndexColumn_ShouldReturnRecords() throws TransactionException {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void scan_ScanGivenForIndexColumnWithConjunctions_ShouldReturnRecords()
      throws TransactionException {}
}
