package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class BatchHandler extends StatementHandler {
  public BatchHandler(ObjectStorageWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
  }

  public void handle(List<? extends Mutation> mutations) throws ExecutionException {
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        new PutStatementHandler(wrapper, metadataManager).handle((Put) mutation);
      } else if (mutation instanceof Delete) {
        new DeleteStatementHandler(wrapper, metadataManager).handle((Delete) mutation);
      } else {
        throw new AssertionError("Unsupported mutation type: " + mutation.getClass().getName());
      }
    }
  }
}
