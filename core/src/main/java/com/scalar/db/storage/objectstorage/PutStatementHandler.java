package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class PutStatementHandler extends StatementHandler {
  public PutStatementHandler(ObjectStorageWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
  }

  public void handle(Put put) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(put);
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, tableMetadata);

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
      String namespaceName,
      String tableName,
      String partitionName,
      String concatenatedKey,
      ObjectStorageMutation mutation)
      throws ExecutionException {
    String objectKey = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName);
    try {
      ObjectStorageWrapperResponse response =
          wrapper.get(ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName));
      Map<String, ObjectStorageRecord> partition =
          JsonConvertor.deserialize(
              response.getValue(), new TypeReference<Map<String, ObjectStorageRecord>>() {});
      if (partition.containsKey(concatenatedKey)) {
        // If the record already exists in the partition, update it
        partition.put(concatenatedKey, mutation.makeRecord(partition.get(concatenatedKey)));
      } else {
        // If the record does not exist in the partition, insert it
        partition.put(concatenatedKey, mutation.makeRecord());
      }
      if (!wrapper.updateIfVersionMatches(
          objectKey, JsonConvertor.serialize(partition), response.getVersion())) {
        // If the partition is updated by another transaction, throw an exception
        throw new RetriableExecutionException(
            CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
                "Conflict occurred"));
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // If the partition does not exist, create it
        try {
          Map<String, ObjectStorageRecord> partition =
              Collections.singletonMap(concatenatedKey, mutation.makeRecord());
          wrapper.insert(objectKey, JsonConvertor.serialize(partition));
        } catch (ObjectStorageWrapperException e2) {
          if (e2.getCode() == ObjectStorageWrapperException.StatusCode.ALREADY_EXISTS
              || e2.getCode() == ObjectStorageWrapperException.StatusCode.CONFLICT) {
            // If the partition is created by another transaction, throw an exception
            throw new RetriableExecutionException(
                CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
                    "Conflict occurred"),
                e2);
          } else {
            throw new ExecutionException(
                CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e2);
          }
        }
      } else {
        throw new ExecutionException(
            CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }

  private void insertRecord(
      String namespaceName,
      String tableName,
      String partitionName,
      String concatenatedKey,
      ObjectStorageMutation mutation)
      throws ExecutionException {
    String objectKey = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName);
    try {
      ObjectStorageWrapperResponse response =
          wrapper.get(ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionName));
      Map<String, ObjectStorageRecord> partition =
          JsonConvertor.deserialize(
              response.getValue(), new TypeReference<Map<String, ObjectStorageRecord>>() {});
      if (partition.containsKey(concatenatedKey)) {
        // If the record already exists in the partition, throw an exception
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record already exists"));
      }
      partition.put(concatenatedKey, mutation.makeRecord());
      if (!wrapper.updateIfVersionMatches(
          objectKey, JsonConvertor.serialize(partition), response.getVersion())) {
        // If the partition is updated by another transaction, throw an exception
        throw new RetriableExecutionException(
            CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
                "Conflict occurred"));
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // If the partition does not exist, create it
        try {
          Map<String, ObjectStorageRecord> partition =
              Collections.singletonMap(concatenatedKey, mutation.makeRecord());
          wrapper.insert(objectKey, JsonConvertor.serialize(partition));
        } catch (ObjectStorageWrapperException e2) {
          if (e2.getCode() == ObjectStorageWrapperException.StatusCode.ALREADY_EXISTS
              || e2.getCode() == ObjectStorageWrapperException.StatusCode.CONFLICT) {
            // If the partition is created by another transaction, throw an exception
            throw new RetriableExecutionException(
                CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
                    "Conflict occurred"),
                e2);
          } else {
            throw new ExecutionException(
                CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e2);
          }
        }
      } else {
        throw new ExecutionException(
            CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }

  private void updateRecord(
      String namespaceName,
      String tableName,
      String partitionName,
      String concatenatedKey,
      ObjectStorageMutation mutation)
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
      partition.put(concatenatedKey, mutation.makeRecord(partition.get(concatenatedKey)));
      if (!wrapper.updateIfVersionMatches(
          objectKey, JsonConvertor.serialize(partition), response.getVersion())) {
        // If the partition is updated by another transaction, throw an exception
        throw new RetriableExecutionException(
            CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
                "Conflict occurred"));
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // If the partition does not exist, throw an exception
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record not found"), e);
      } else {
        throw new ExecutionException(
            CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
      }
    } catch (Exception e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(), e);
    }
  }

  private void conditionalUpdateRecord(
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
      partition.put(concatenatedKey, mutation.makeRecord(partition.get(concatenatedKey)));
      if (!wrapper.updateIfVersionMatches(
          objectKey, JsonConvertor.serialize(partition), response.getVersion())) {
        // If the partition is updated by another transaction, throw an exception
        throw new RetriableExecutionException(
            CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
                "Conflict occurred"));
      }
    } catch (ExecutionException e) {
      throw e;
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        // If the partition does not exist, throw an exception
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record not found"), e);
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
