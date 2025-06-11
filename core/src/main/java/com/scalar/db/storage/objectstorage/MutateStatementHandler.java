package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Mutation;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MutateStatementHandler extends StatementHandler {
  private final Map<PartitionIdentifier, String> readVersionMap;

  public MutateStatementHandler(
      ObjectStorageWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
    this.readVersionMap = new HashMap<>();
  }

  public void handle(Mutation mutation) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
    ObjectStorageMutation objectStorageMutation =
        new ObjectStorageMutation(mutation, tableMetadata);
    mutate(
        getNamespace(mutation),
        getTable(mutation),
        objectStorageMutation.getConcatenatedPartitionKey(),
        Collections.singletonList(mutation));
  }

  public void handle(List<? extends Mutation> mutations) throws ExecutionException {
    Map<PartitionIdentifier, List<Mutation>> mutationPerPartition = new HashMap<>();
    for (Mutation mutation : mutations) {
      TableMetadata tableMetadata = metadataManager.getTableMetadata(mutation);
      ObjectStorageMutation objectStorageMutation =
          new ObjectStorageMutation(mutation, tableMetadata);
      String partitionKey = objectStorageMutation.getConcatenatedPartitionKey();
      PartitionIdentifier partitionIdentifier =
          PartitionIdentifier.of(getNamespace(mutation), getTable(mutation), partitionKey);
      mutationPerPartition
          .computeIfAbsent(partitionIdentifier, k -> new ArrayList<>())
          .add(mutation);
    }
    for (Map.Entry<PartitionIdentifier, List<Mutation>> entry : mutationPerPartition.entrySet()) {
      PartitionIdentifier partitionIdentifier = entry.getKey();
      mutate(
          partitionIdentifier.getNamespaceName(),
          partitionIdentifier.getTableName(),
          partitionIdentifier.getPartitionName(),
          entry.getValue());
    }
  }

  public void mutate(
      String namespaceName, String tableName, String partitionKey, List<Mutation> mutations)
      throws ExecutionException {
    Map<String, ObjectStorageRecord> partition =
        getPartition(namespaceName, tableName, partitionKey);
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        putInternal(partition, (Put) mutation);
      } else {
        assert mutation instanceof Delete;
        deleteInternal(partition, (Delete) mutation);
      }
    }
    applyPartitionWrite(namespaceName, tableName, partitionKey, partition);
  }

  private void applyPartitionWrite(
      String namespaceName,
      String tableName,
      String partitionKey,
      Map<String, ObjectStorageRecord> partition)
      throws ExecutionException {
    if (readVersionMap.containsKey(
        PartitionIdentifier.of(namespaceName, tableName, partitionKey))) {
      if (!partition.isEmpty()) {
        updatePartition(namespaceName, tableName, partitionKey, partition);
      } else {
        deletePartition(namespaceName, tableName, partitionKey);
      }
    } else {
      if (!partition.isEmpty()) {
        insertPartition(namespaceName, tableName, partitionKey, partition);
      }
    }
  }

  private Map<String, ObjectStorageRecord> getPartition(
      String namespaceName, String tableName, String partitionKey) throws ExecutionException {
    String objectKey = ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey);
    try {
      ObjectStorageWrapperResponse response = wrapper.get(objectKey);
      readVersionMap.put(
          PartitionIdentifier.of(namespaceName, tableName, partitionKey), response.getVersion());
      return JsonConvertor.deserialize(
          response.getValue(), new TypeReference<Map<String, ObjectStorageRecord>>() {});
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.NOT_FOUND) {
        return new HashMap<>();
      }
      throw new ExecutionException(
          String.format("Failed to get partition for object key '%s'", objectKey), e);
    }
  }

  private void putInternal(Map<String, ObjectStorageRecord> partition, Put put)
      throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(put);
    ObjectStorageMutation mutation = new ObjectStorageMutation(put, tableMetadata);
    if (!put.getCondition().isPresent()) {
      ObjectStorageRecord existingRecord = partition.get(mutation.getConcatenatedKey());
      if (existingRecord == null) {
        partition.put(mutation.getConcatenatedKey(), mutation.makeRecord());
      } else {
        partition.put(mutation.getConcatenatedKey(), mutation.makeRecord(existingRecord));
      }
    } else if (put.getCondition().get() instanceof PutIfNotExists) {
      if (partition.containsKey(mutation.getConcatenatedKey())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record already exists"));
      }
      partition.put(mutation.getConcatenatedKey(), mutation.makeRecord());
    } else if (put.getCondition().get() instanceof PutIfExists) {
      ObjectStorageRecord existingRecord = partition.get(mutation.getConcatenatedKey());
      if (existingRecord == null) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record not found"));
      }
      partition.put(mutation.getConcatenatedKey(), mutation.makeRecord(existingRecord));
    } else {
      assert put.getCondition().get() instanceof PutIf;
      ObjectStorageRecord existingRecord = partition.get(mutation.getConcatenatedKey());
      if (existingRecord == null) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record not found"));
      }
      if (!areConditionsMet(
          partition.get(mutation.getConcatenatedKey()),
          put.getCondition().get().getExpressions(),
          metadataManager.getTableMetadata(mutation.getOperation()))) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Conditions not met"));
      }
      partition.put(mutation.getConcatenatedKey(), mutation.makeRecord(existingRecord));
    }
  }

  private void deleteInternal(Map<String, ObjectStorageRecord> partition, Delete delete)
      throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(delete);
    ObjectStorageMutation mutation = new ObjectStorageMutation(delete, tableMetadata);
    if (!delete.getCondition().isPresent()) {
      partition.remove(mutation.getConcatenatedKey());
    } else if (delete.getCondition().get() instanceof DeleteIfExists) {
      if (!partition.containsKey(mutation.getConcatenatedKey())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record not found"));
      }
      partition.remove(mutation.getConcatenatedKey());
    } else {
      assert delete.getCondition().get() instanceof DeleteIf;
      if (!partition.containsKey(mutation.getConcatenatedKey())) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Record not found"));
      }
      if (!areConditionsMet(
          partition.get(mutation.getConcatenatedKey()),
          delete.getCondition().get().getExpressions(),
          metadataManager.getTableMetadata(mutation.getOperation()))) {
        throw new NoMutationException(
            CoreError.NO_MUTATION_APPLIED.buildMessage("Conditions not met"));
      }
      partition.remove(mutation.getConcatenatedKey());
    }
  }

  private void insertPartition(
      String namespaceName,
      String tableName,
      String partitionKey,
      Map<String, ObjectStorageRecord> partition)
      throws ExecutionException {
    try {
      wrapper.insert(
          ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey),
          JsonConvertor.serialize(partition));
    } catch (ObjectStorageWrapperException e) {
      if (e.getCode() == ObjectStorageWrapperException.StatusCode.CONFLICT
          || e.getCode() == ObjectStorageWrapperException.StatusCode.ALREADY_EXISTS) {
        throw new RetriableExecutionException(
            CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
                "Conflict occurred while inserting partition"),
            e);
      }
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_MUTATION.buildMessage(
              "Failed to insert partition"),
          e);
    }
  }

  private void updatePartition(
      String namespaceName,
      String tableName,
      String partitionKey,
      Map<String, ObjectStorageRecord> partition)
      throws ExecutionException {
    if (!wrapper.updateIfVersionMatches(
        ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey),
        JsonConvertor.serialize(partition),
        readVersionMap.get(PartitionIdentifier.of(namespaceName, tableName, partitionKey)))) {
      throw new RetriableExecutionException(
          CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
              "Conflict occurred while updating partition"));
    }
  }

  private void deletePartition(String namespaceName, String tableName, String partitionKey)
      throws ExecutionException {
    if (!wrapper.deleteIfVersionMatches(
        ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey),
        readVersionMap.get(PartitionIdentifier.of(namespaceName, tableName, partitionKey)))) {
      throw new RetriableExecutionException(
          CoreError.OBJECT_STORAGE_TRANSACTION_CONFLICT_OCCURRED_IN_MUTATION.buildMessage(
              "Conflict occurred while deleting partition"));
    }
  }
}
