package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.Utils;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.RecordKey;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationRecordRepository {
  private static final Logger logger = LoggerFactory.getLogger(ReplicationRecordRepository.class);
  private static final TypeReference<Set<Value>> typeRefForValueInRecords =
      new TypeReference<Set<Value>>() {};
  private static final TypeReference<Set<String>> typeRefForInsertTxIdsInRecords =
      new TypeReference<Set<String>>() {};

  private final DistributedStorage replicationDbStorage;
  private final ObjectMapper objectMapper;
  private final String replicationDbNamespace;
  private final String replicationDbRecordsTable;
  private final String emptySet;

  public ReplicationRecordRepository(
      DistributedStorage replicationDbStorage,
      ObjectMapper objectMapper,
      String replicationDbNamespace,
      String replicationDbRecordsTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.objectMapper = objectMapper;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbRecordsTable = replicationDbRecordsTable;
    try {
      emptySet = objectMapper.writeValueAsString(Collections.EMPTY_SET);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize an empty set");
    }
  }

  public String serializeRecordKey(RecordKey key) {
    try {
      return objectMapper.writeValueAsString(key);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize a key. Key:%s", e);
    }
  }

  public Optional<Record> get(RecordKey key) throws ExecutionException {
    Optional<Result> result =
        replicationDbStorage.get(
            Get.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbRecordsTable)
                // TODO: Provision for performance
                .partitionKey(Key.ofText("key", serializeRecordKey(key)))
                .build());

    return result.flatMap(
        r -> {
          try {
            Record record =
                new Record(
                    objectMapper.readValue(
                        r.getText("key"),
                        com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model
                            .Record.RecordKey.class),
                    r.getBigInt("version"),
                    r.getText("current_tx_id"),
                    r.getText("prep_tx_id"),
                    r.getBoolean("deleted"),
                    objectMapper.readValue(r.getText("values"), typeRefForValueInRecords),
                    objectMapper.readValue(
                        r.getText("insert_tx_ids"), typeRefForInsertTxIdsInRecords),
                    Instant.ofEpochMilli(r.getBigInt("appended_at")),
                    Instant.ofEpochMilli(r.getBigInt("shrinked_at")));

            logger.debug("[get]\n  key:{}\n  record:{}\n", key, record.toStringOnlyWithMetadata());

            return Optional.of(record);
          } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize `values` for key:" + key, e);
          }
        });
  }

  private long setCasCondition(Buildable buildable, Optional<Record> existingRecord) {
    long nextVersion;

    if (existingRecord.isPresent()) {
      long currentVersion = existingRecord.get().version;
      nextVersion = currentVersion + 1;
      buildable.value(BigIntColumn.of("version", nextVersion));
      buildable.condition(
          ConditionBuilder.putIf(
                  ConditionBuilder.buildConditionalExpression(
                      BigIntColumn.of("version", currentVersion), Operator.EQ))
              .build());
    } else {
      nextVersion = 1;
      buildable.value(BigIntColumn.of("version", nextVersion));
      buildable.condition(ConditionBuilder.putIfNotExists());
    }

    return nextVersion;
  }

  public Record upsertWithNewValue(RecordKey key, Value newValue) throws ExecutionException {
    Optional<Record> recordOpt = get(key);

    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(Key.ofText("key", serializeRecordKey(key)));
    long nextVersion = setCasCondition(putBuilder, recordOpt);

    Set<Value> values = new HashSet<>();
    recordOpt.ifPresent(record -> values.addAll(record.values));
    if (!values.add(newValue)) {
      // TODO: In this case, updating the record isn't needed?
      logger.warn("The new value is already stored. key:{}, txId:{}", key, newValue.txId);
    }

    if (!recordOpt.isPresent()) {
      putBuilder.textValue("insert_tx_ids", emptySet);
    }

    long appendedAtMillis = System.currentTimeMillis();
    try {
      logger.debug(
          "[upsertWithNewValue]\n  key:{}\n  values={}\n", key, Utils.convValuesToString(values));

      replicationDbStorage.put(
          putBuilder
              .textValue("values", objectMapper.writeValueAsString(values))
              .bigIntValue("appended_at", appendedAtMillis)
              .build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize `values` for key:" + key, e);
    }

    String curTxId;
    String prepTxId;
    boolean deleted;
    Set<String> insertTxIds;
    Instant shrinkedAt;
    if (recordOpt.isPresent()) {
      Record record = recordOpt.get();
      curTxId = record.currentTxId;
      prepTxId = record.prepTxId;
      deleted = record.deleted;
      insertTxIds = record.insertTxIds;
      shrinkedAt = record.shrinkedAt;
    } else {
      curTxId = null;
      prepTxId = null;
      deleted = false;
      insertTxIds = Collections.emptySet();
      shrinkedAt = null;
    }

    return new Record(
        key,
        nextVersion,
        curTxId,
        prepTxId,
        deleted,
        values,
        insertTxIds,
        Instant.ofEpochMilli(appendedAtMillis),
        shrinkedAt);
  }

  public void updateWithValues(
      Record record,
      @Nullable String newTxId,
      boolean deleted,
      Collection<Value> values,
      Collection<String> newInsertions)
      throws ExecutionException {
    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(Key.ofText("key", serializeRecordKey(record.key)));
    setCasCondition(putBuilder, Optional.of(record));

    if (newTxId != null && newTxId.equals(record.currentTxId)) {
      logger.warn("`tx_id` isn't changed. old:{}, new:{}", record.currentTxId, newTxId);
    }

    if (!newInsertions.isEmpty()) {
      Set<String> insertedTxIds = new HashSet<>();
      insertedTxIds.addAll(record.insertTxIds);
      insertedTxIds.addAll(newInsertions);
      try {
        putBuilder.textValue("insert_tx_ids", objectMapper.writeValueAsString(insertedTxIds));
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize `insert_tx_ids` for key:" + record.key, e);
      }
    }

    try {
      logger.debug(
          "[updateWithValues]\n  key:{}\n  curVer:{}\n  newTxId:{}\n  prepTxId:{}\n  values={}\n",
          record.key,
          record.version,
          newTxId,
          record.prepTxId,
          Utils.convValuesToString(values));

      replicationDbStorage.put(
          putBuilder
              .textValue("values", objectMapper.writeValueAsString(values))
              .textValue("current_tx_id", newTxId)
              // Clear `prep_tx_id` for subsequent transactions
              .textValue("prep_tx_id", null)
              .booleanValue("deleted", deleted)
              .build());
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize `values` for key:" + record.key, e);
    }
  }

  public void updateWithPrepTxId(Record record, String prepTxId) throws ExecutionException {
    long currentVersion = record.version;
    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(Key.ofText("key", serializeRecordKey(record.key)));
    putBuilder.condition(
        ConditionBuilder.putIf(
            Arrays.asList(
                ConditionBuilder.buildConditionalExpression(
                    BigIntColumn.of("version", currentVersion), Operator.EQ),
                ConditionBuilder.buildConditionalExpression(
                    TextColumn.of("prep_tx_id", null), Operator.IS_NULL))));

    logger.debug(
        "[updatePrepTxId]\n  record:{}\n  prepTxId:{}\n",
        record.toStringOnlyWithMetadata(),
        prepTxId);

    replicationDbStorage.put(putBuilder.textValue("prep_tx_id", prepTxId).build());
  }
}
