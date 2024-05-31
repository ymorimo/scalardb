package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedRecord;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class ReplicationUpdatedRecordRepository {
  private final DistributedStorage replicationDbStorage;
  private final ObjectMapper objectMapper;
  private final String replicationDbNamespace;
  private final String replicationDbUpdatedRecordTable;

  public ReplicationUpdatedRecordRepository(
      DistributedStorage replicationDbStorage,
      ObjectMapper objectMapper,
      String replicationDbNamespace,
      String replicationDbUpdatedRecordTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.objectMapper = objectMapper;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbUpdatedRecordTable = replicationDbUpdatedRecordTable;
  }

  public List<UpdatedRecord> scan(int partitionId, int fetchSize)
      throws ExecutionException, IOException {
    try (Scanner scan =
        replicationDbStorage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbUpdatedRecordTable)
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("updated_at"))
                .limit(fetchSize)
                .build())) {
      return scan.all().stream()
          .map(
              result -> {
                try {
                  return new UpdatedRecord(
                      partitionId,
                      result.getText("namespace"),
                      result.getText("table"),
                      objectMapper.readValue(
                          result.getText("pk"),
                          com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model
                              .Key.class),
                      objectMapper.readValue(
                          result.getText("ck"),
                          com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model
                              .Key.class),
                      Instant.ofEpochMilli(result.getBigInt("updated_at")));
                } catch (JsonProcessingException e) {
                  throw new RuntimeException(e);
                }
              })
          .collect(Collectors.toList());
    }
  }

  private Put createPutFromUpdatedRecord(UpdatedRecord updatedRecord) {
    try {
      return Put.newBuilder()
          .namespace(replicationDbNamespace)
          .table(replicationDbUpdatedRecordTable)
          .partitionKey(Key.ofInt("partition_id", updatedRecord.partitionId))
          .clusteringKey(
              Key.newBuilder()
                  .addBigInt("updated_at", updatedRecord.updatedAt.toEpochMilli())
                  .addText("namespace", updatedRecord.namespace)
                  .addText("table", updatedRecord.table)
                  .addText("pk", objectMapper.writeValueAsString(updatedRecord.pk))
                  .addText("ck", objectMapper.writeValueAsString(updatedRecord.ck))
                  .build())
          .build();
    } catch (JsonProcessingException e) {
      throw new RuntimeException(
          String.format("Failed to create a key for record. UpdatedRecord:%s", updatedRecord), e);
    }
  }

  public void add(UpdatedRecord updatedRecord) throws ExecutionException {
    replicationDbStorage.put(createPutFromUpdatedRecord(updatedRecord));
  }
}
