package com.scalar.db.storage.blob;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class PutStatementHandler extends StatementHandler {
  public PutStatementHandler(BlobClientWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
  }

  public void handle(Put put) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(put);
    BlobMutation mutation = new BlobMutation(put, tableMetadata);

    if (!put.getCondition().isPresent()) {
      upsertRecord(
          getNamespace(put),
          getTable(put),
          mutation.getConcatenatedPartitionKey(),
          mutation.getConcatenatedKey(),
          mutation);
    } else if (put.getCondition().get() instanceof PutIfNotExists) {
      insertRecord(
          getNamespace(put),
          getTable(put),
          mutation.getConcatenatedPartitionKey(),
          mutation.getConcatenatedKey(),
          mutation);
    } else if (put.getCondition().get() instanceof PutIfExists) {
      updateRecord(
          getNamespace(put),
          getTable(put),
          mutation.getConcatenatedPartitionKey(),
          mutation.getConcatenatedKey(),
          mutation);
    } else {
      assert put.getCondition().get() instanceof PutIf;
      conditionalUpdateRecord(
          getNamespace(put),
          getTable(put),
          mutation.getConcatenatedPartitionKey(),
          mutation.getConcatenatedKey(),
          mutation,
          put.getCondition().get().getExpressions());
    }
  }

  private void upsertRecord(
      String namespace,
      String table,
      String partition,
      String concatenatedKey,
      BlobMutation mutation)
      throws ExecutionException {
    String objectKey = BlobUtils.getObjectKey(namespace, table, partition, concatenatedKey);
    try {
      BlobClientWrapperResponse response = wrapper.get(objectKey);
      BlobRecord currentRecord = JsonConvertor.deserialize(response.getValue(), BlobRecord.class);
      BlobRecord record = mutation.makeRecord(currentRecord);
      if (!wrapper.compareAndSwap(objectKey, JsonConvertor.serialize(record), response.getETag())) {
        throw new RetriableExecutionException(
            CoreError.BLOB_CONFLICT_OCCURRED_IN_MUTATION.buildMessage());
      }
    } catch (BlobClientWrapperException e) {
      if (e.getCode() == BlobClientWrapperException.StatusCode.NOT_FOUND) {
        try {
          BlobRecord record = mutation.makeRecord();
          wrapper.insert(objectKey, JsonConvertor.serialize(record));
        } catch (BlobClientWrapperException e2) {
          if (e2.getCode() == BlobClientWrapperException.StatusCode.ALREADY_EXISTS
              || e2.getCode() == BlobClientWrapperException.StatusCode.CONFLICT) {
            throw new RetriableExecutionException(
                CoreError.BLOB_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(), e2);
          } else {
            throw new ExecutionException(
                CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e2);
          }
        } catch (Exception e2) {
          throw new ExecutionException(
              CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e2);
        }
      } else {
        throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (Exception e) {
      throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }

  private void insertRecord(
      String namespace,
      String table,
      String partition,
      String concatenatedKey,
      BlobMutation mutation)
      throws ExecutionException {
    String objectKey = BlobUtils.getObjectKey(namespace, table, partition, concatenatedKey);
    try {
      BlobRecord record = mutation.makeRecord();
      wrapper.insert(objectKey, JsonConvertor.serialize(record));
    } catch (BlobClientWrapperException e) {
      if (e.getCode() == BlobClientWrapperException.StatusCode.CONFLICT) {
        throw new RetriableExecutionException(
            CoreError.BLOB_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(), e);
      } else if (e.getCode() == BlobClientWrapperException.StatusCode.ALREADY_EXISTS) {
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage(), e);
      } else {
        throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (Exception e) {
      throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }

  private void updateRecord(
      String namespace,
      String table,
      String partition,
      String concatenatedKey,
      BlobMutation mutation)
      throws ExecutionException {
    String objectKey = BlobUtils.getObjectKey(namespace, table, partition, concatenatedKey);
    try {
      BlobClientWrapperResponse response = wrapper.get(objectKey);
      BlobRecord currentRecord = JsonConvertor.deserialize(response.getValue(), BlobRecord.class);
      BlobRecord record = mutation.makeRecord(currentRecord);
      if (!wrapper.compareAndSwap(objectKey, JsonConvertor.serialize(record), response.getETag())) {
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

  private void conditionalUpdateRecord(
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
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Conditions not met"));
      }
      BlobRecord record = mutation.makeRecord(currentRecord);
      if (!wrapper.compareAndSwap(objectKey, JsonConvertor.serialize(record), response.getETag())) {
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
