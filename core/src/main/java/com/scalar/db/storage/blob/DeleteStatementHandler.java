package com.scalar.db.storage.blob;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class DeleteStatementHandler extends StatementHandler {
  public DeleteStatementHandler(BlobClientWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
  }

  public void handle(Delete delete) throws ExecutionException {
    TableMetadata metadata = metadataManager.getTableMetadata(delete);
    BlobMutation mutation = new BlobMutation(delete, metadata);

    if (!delete.getCondition().isPresent()) {
      deleteRecordIfExists(
          getNamespace(delete),
          getTable(delete),
          mutation.getConcatenatedPartitionKey(),
          mutation.getConcatenatedKey());
    } else if (delete.getCondition().get() instanceof DeleteIfExists) {
      deleteRecord(
          getNamespace(delete),
          getTable(delete),
          mutation.getConcatenatedPartitionKey(),
          mutation.getConcatenatedKey());
    } else {
      assert delete.getCondition().get() instanceof DeleteIf;
      conditionalDeleteRecord(
          getNamespace(delete),
          getTable(delete),
          mutation.getConcatenatedPartitionKey(),
          mutation.getConcatenatedKey(),
          mutation,
          delete.getCondition().get().getExpressions());
    }
  }

  private void deleteRecord(
      String namespace, String table, String partition, String concatenatedKey)
      throws ExecutionException {
    String objectKey = BlobUtils.getObjectKey(namespace, table, partition, concatenatedKey);
    try {
      wrapper.delete(objectKey);
    } catch (BlobClientWrapperException e) {
      if (e.getCode() == BlobClientWrapperException.StatusCode.NOT_FOUND) {
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage(), e);
      } else {
        throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (Exception e) {
      throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }

  private void deleteRecordIfExists(
      String namespace, String table, String partition, String concatenatedKey)
      throws ExecutionException {
    String objectKey = BlobUtils.getObjectKey(namespace, table, partition, concatenatedKey);
    try {
      wrapper.deleteIfExists(objectKey);
    } catch (Exception e) {
      throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }

  private void conditionalDeleteRecord(
      String namespace,
      String table,
      String partition,
      String concatenatedKey,
      BlobMutation mutation,
      List<ConditionalExpression> expressions)
      throws ExecutionException {
    String objectKey = BlobUtils.getObjectKey(namespace, table, partition, concatenatedKey);
    try {
      BlobClientWrapperResponse response = wrapper.get(objectKey);
      BlobRecord currentRecord = JsonConvertor.deserialize(response.getValue(), BlobRecord.class);

      if (!areConditionsMet(
          currentRecord, expressions, metadataManager.getTableMetadata(mutation.getOperation()))) {
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage());
      }

      if (!wrapper.compareAndDelete(objectKey, response.getETag())) {
        throw new RetriableExecutionException(
            CoreError.BLOB_CONFLICT_OCCURRED_IN_MUTATION.buildMessage());
      }
    } catch (BlobClientWrapperException e) {
      if (e.getCode() == BlobClientWrapperException.StatusCode.NOT_FOUND) {
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage(), e);
      } else {
        throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (Exception e) {
      throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }
}
