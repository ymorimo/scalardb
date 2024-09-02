package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
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
  private final String replicationDbNamespace;
  private final String replicationDbRecordsTable;
  private final String emptySet = JSON.toJSONString(Collections.emptySet());

  public ReplicationRecordRepository(
      DistributedStorage replicationDbStorage,
      String replicationDbNamespace,
      String replicationDbRecordsTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbRecordsTable = replicationDbRecordsTable;
  }

  public String serializeRecordKey(RecordKey key) {
    return JSON.toJSONString(key);
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
          Record record =
              new Record(
                  JSON.parseObject(
                      r.getText("key"),
                      com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model
                          .Record.RecordKey.class),
                  r.getBigInt("version"),
                  r.getText("current_tx_id"),
                  r.getText("prep_tx_id"),
                  r.getBoolean("deleted"),
                  JSON.parseObject(r.getText("values"), typeRefForValueInRecords),
                  JSON.parseObject(r.getText("insert_tx_ids"), typeRefForInsertTxIdsInRecords),
                  Instant.ofEpochMilli(r.getBigInt("appended_at")),
                  Instant.ofEpochMilli(r.getBigInt("shrinked_at")));

          logger.debug("[get]\n  key:{}\n  record:{}\n", key, record.toStringOnlyWithMetadata());

          return Optional.of(record);
        });
  }

  private void setCasCondition(Buildable buildable, Optional<Record> existingRecord) {
    if (existingRecord.isPresent()) {
      long currentVersion = existingRecord.get().version;
      buildable.value(BigIntColumn.of("version", currentVersion + 1));
      buildable.condition(
          ConditionBuilder.putIf(
                  ConditionBuilder.buildConditionalExpression(
                      BigIntColumn.of("version", currentVersion), Operator.EQ))
              .build());
    } else {
      buildable.value(BigIntColumn.of("version", 1));
      buildable.condition(ConditionBuilder.putIfNotExists());
    }
  }

  public void upsertWithNewValue(RecordKey key, Value newValue) throws ExecutionException {
    Optional<Record> recordOpt = get(key);

    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(Key.ofText("key", serializeRecordKey(key)));
    setCasCondition(putBuilder, recordOpt);

    Set<Value> values = new HashSet<>();
    recordOpt.ifPresent(record -> values.addAll(record.values));
    if (!values.add(newValue)) {
      logger.warn("The new value is already stored. key:{}, txId:{}", key, newValue.txId);
    }

    if (!recordOpt.isPresent()) {
      putBuilder.textValue("insert_tx_ids", emptySet);
    }

    logger.debug(
        "[upsertWithNewValue]\n  key:{}\n  values={}\n", key, Utils.convValuesToString(values));

    replicationDbStorage.put(putBuilder.textValue("values", JSON.toJSONString(values)).build());
  }

  public void updateWithValues(
      RecordKey key,
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
            .partitionKey(Key.ofText("key", serializeRecordKey(key)));
    setCasCondition(putBuilder, Optional.of(record));

    if (newTxId != null && newTxId.equals(record.currentTxId)) {
      logger.warn("`tx_id` isn't changed. old:{}, new:{}", record.currentTxId, newTxId);
    }

    if (!newInsertions.isEmpty()) {
      Set<String> insertedTxIds = new HashSet<>();
      insertedTxIds.addAll(record.insertTxIds);
      insertedTxIds.addAll(newInsertions);
      putBuilder.textValue("insert_tx_ids", JSON.toJSONString(insertedTxIds));
    }

    logger.debug(
        "[updateWithValues]\n  key:{}\n  curVer:{}\n  newTxId:{}\n  prepTxId:{}\n  values={}\n",
        key,
        record.version,
        newTxId,
        record.prepTxId,
        Utils.convValuesToString(values));

    replicationDbStorage.put(
        putBuilder
            .textValue("values", JSON.toJSONString(values))
            .textValue("current_tx_id", newTxId)
            // Clear `prep_tx_id` for subsequent transactions
            .textValue("prep_tx_id", null)
            .booleanValue("deleted", deleted)
            .build());
  }

  public void updateWithPrepTxId(RecordKey key, Record record, String prepTxId)
      throws ExecutionException {
    long currentVersion = record.version;
    Buildable putBuilder =
        Put.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbRecordsTable)
            .partitionKey(Key.ofText("key", serializeRecordKey(key)));
    putBuilder.condition(
        ConditionBuilder.putIf(
            Arrays.asList(
                ConditionBuilder.buildConditionalExpression(
                    BigIntColumn.of("version", currentVersion), Operator.EQ),
                ConditionBuilder.buildConditionalExpression(
                    TextColumn.of("prep_tx_id", null), Operator.IS_NULL))));

    logger.debug(
        "[updatePrepTxId]\n  key:{}\n  curVer:{}\n  prepTxId:{}\n", key, record.version, prepTxId);

    replicationDbStorage.put(putBuilder.textValue("prep_tx_id", prepTxId).build());
  }
}
