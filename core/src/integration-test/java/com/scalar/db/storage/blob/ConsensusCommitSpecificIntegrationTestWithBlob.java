package com.scalar.db.storage.blob;

import com.scalar.db.transaction.consensuscommit.ConsensusCommitSpecificIntegrationTestBase;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConsensusCommitSpecificIntegrationTestWithBlob
    extends ConsensusCommitSpecificIntegrationTestBase {

  @Override
  protected Properties getProperties(String testName) {
    return ConsensusCommitBlobEnv.getProperties(testName);
  }

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void
      scanWithIndex_PutWithOverlappedIndexKeyAndNonOverlappedConjunctionsGivenBefore_ShouldScan() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void
      scanWithIndex_OverlappingPutWithNonIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void
      scanWithIndex_NonOverlappingPutWithIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void
      scanWithIndex_OverlappingPutWithIndexedColumnGivenBefore_ShouldThrowIllegalArgumentException() {}

  @Test
  @Override
  @Disabled("Index-related operations are not supported for Blob")
  public void
      scanWithIndex_OverlappingPutWithIndexedColumnAndConjunctionsGivenBefore_ShouldThrowIllegalArgumentException() {}
}
