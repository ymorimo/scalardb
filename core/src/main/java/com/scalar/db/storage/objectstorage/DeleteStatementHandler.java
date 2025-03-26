package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class DeleteStatementHandler extends StatementHandler {
  public DeleteStatementHandler(
      ObjectStorageWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
  }

  public void handle(Delete delete) throws ExecutionException {
    TableMetadata metadata = metadataManager.getTableMetadata(delete);
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, metadata);

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
      String namespaceName, String tableName, String partitionName, String concatenatedKey)
      throws ExecutionException {
    String objectKey = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName);
    try {
      ObjectStorageWrapperResponse response =
          wrapper.get(ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName));
      Map<String, ObjectStorageRecord> partition =
          JsonConvertor.deserialize(
              response.getValue(), new TypeReference<Map<String, ObjectStorageRecord>>() {});
      if (!partition.containsKey(concatenatedKey)) {
        // If the record does not exist in the partition, throw an exception
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record not found"));
      }
      partition.remove(concatenatedKey);
      if (partition.isEmpty()) {
        // If the partition is empty, delete the partition
        if (!wrapper.deleteIfVersionMatches(objectKey, response.getVersion())) {
          // If the partition is updated by another transaction, throw an exception
          throw new RetriableExecutionException(
              CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage());
        }
      } else {
        if (!wrapper.updateIfVersionMatches(
            objectKey, JsonConvertor.serialize(partition), response.getVersion())) {
          // If the partition is updated by another transaction, throw an exception
          throw new RetriableExecutionException(
              CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage());
        }
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // If the partition does not exist, throw an exception
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage(), e);
      } else {
        throw new ExecutionException(
            CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }

  private void deleteRecordIfExists(
      String namespaceName, String tableName, String partitionName, String concatenatedKey)
      throws ExecutionException {
    String objectKey = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName);
    try {
      ObjectStorageWrapperResponse response =
          wrapper.get(ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName));
      Map<String, ObjectStorageRecord> partition =
          JsonConvertor.deserialize(
              response.getValue(), new TypeReference<Map<String, ObjectStorageRecord>>() {});
      if (partition.containsKey(concatenatedKey)) {
        partition.remove(concatenatedKey);
        if (partition.isEmpty()) {
          // If the partition is empty, delete the partition
          if (!wrapper.deleteIfVersionMatches(objectKey, response.getVersion())) {
            // If the partition is updated by another transaction, throw an exception
            throw new RetriableExecutionException(
                CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage());
          }
        } else {
          if (!wrapper.updateIfVersionMatches(
              objectKey, JsonConvertor.serialize(partition), response.getVersion())) {
            // If the partition is updated by another transaction, throw an exception
            throw new RetriableExecutionException(
                CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage());
          }
        }
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() != ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        throw new ExecutionException(
            CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }

  private void conditionalDeleteRecord(
      String namespaceName,
      String tableName,
      String partitionName,
      String concatenatedKey,
      ObjectStorageMutation mutation,
      List<ConditionalExpression> expressions)
      throws ExecutionException {
    String objectKey = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName);
    try {
      ObjectStorageWrapperResponse response =
          wrapper.get(ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName));
      Map<String, ObjectStorageRecord> partition =
          JsonConvertor.deserialize(
              response.getValue(), new TypeReference<Map<String, ObjectStorageRecord>>() {});
      if (!partition.containsKey(concatenatedKey)) {
        // If the record does not exist in the partition, throw an exception
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record not found"));
      }
      if (!areConditionsMet(
          partition.get(concatenatedKey),
          expressions,
          metadataManager.getTableMetadata(mutation.getOperation()))) {
        // If the conditions are not met, throw an exception
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Conditions not met"));
      }
      partition.remove(concatenatedKey);
      if (partition.isEmpty()) {
        // If the partition is empty, delete the partition
        if (!wrapper.deleteIfVersionMatches(objectKey, response.getVersion())) {
          // If the partition is updated by another transaction, throw an exception
          throw new RetriableExecutionException(
              CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage());
        }
      } else {
        if (!wrapper.updateIfVersionMatches(
            objectKey, JsonConvertor.serialize(partition), response.getVersion())) {
          // If the partition is updated by another transaction, throw an exception
          throw new RetriableExecutionException(
              CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage());
        }
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // If the partition does not exist, throw an exception
        throw new NoMutationException(CoreError.NO_MUTATION_APPLIED.buildMessage(), e);
      } else {
        throw new ExecutionException(
            CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }
}
