package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.RecordWriterThread.KeyHandler;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecordWriterThreadTest {
  private final com.scalar.db.io.Key key =
      com.scalar.db.io.Key.newBuilder()
          .add(TextColumn.of("pk", "pk1"))
          .add(TextColumn.of("ck", "ck1"))
          .build();
  private final Key pk = new Key(new Column<>("pk", "pk1"));
  private final Key ck = new Key(new Column<>("ck", "ck1"));
  @Mock private ReplicationRecordRepository replRecordRepo;
  @Mock private DistributedStorage storage;
  @LazyInit private MetricsLogger metricsLogger;
  @LazyInit private KeyHandler keyHandler;
  @LazyInit private long preparedAtInMillisOfLastValue;
  @LazyInit private long committedAtInMillisOfLastValue;

  @BeforeEach
  void setUp() {
    metricsLogger = new MetricsLogger(new LinkedBlockingQueue<>());
    keyHandler = new KeyHandler(replRecordRepo, storage, metricsLogger);
    preparedAtInMillisOfLastValue = System.currentTimeMillis() - 400;
    committedAtInMillisOfLastValue = System.currentTimeMillis() - 200;
  }

  private void assertStoragePutOperationMetadata(Put put, String fullTableName, Key pk, Key ck) {
    assertThat(put.forFullTableName()).isPresent().isEqualTo(Optional.of(fullTableName));
    assertThat(put.getPartitionKey().getColumns().size()).isEqualTo(pk.columns.size());
    for (int i = 0; i < pk.columns.size(); i++) {
      assertThat(put.getPartitionKey().getColumnName(i)).isEqualTo(pk.columns.get(i).name);
      assertThat(put.getPartitionKey().getTextValue(i)).isEqualTo(pk.columns.get(i).value);
    }
    assertThat(put.getClusteringKey()).isPresent();
    assertThat(put.getClusteringKey().get().size()).isEqualTo(ck.columns.size());
    for (int i = 0; i < ck.columns.size(); i++) {
      assertThat(put.getClusteringKey().get().getColumnName(i)).isEqualTo(ck.columns.get(i).name);
      assertThat(put.getClusteringKey().get().getTextValue(i)).isEqualTo(ck.columns.get(i).value);
    }
  }

  private void assertStorageGetOperationMetadata(Get get, String fullTableName, Key pk, Key ck) {
    assertThat(get.forFullTableName()).isPresent().isEqualTo(Optional.of(fullTableName));
    assertThat(get.getPartitionKey().getColumns().size()).isEqualTo(pk.columns.size());
    for (int i = 0; i < pk.columns.size(); i++) {
      assertThat(get.getPartitionKey().getColumnName(i)).isEqualTo(pk.columns.get(i).name);
      assertThat(get.getPartitionKey().getTextValue(i)).isEqualTo(pk.columns.get(i).value);
    }
    assertThat(get.getClusteringKey()).isPresent();
    assertThat(get.getClusteringKey().get().size()).isEqualTo(ck.columns.size());
    for (int i = 0; i < ck.columns.size(); i++) {
      assertThat(get.getClusteringKey().get().getColumnName(i)).isEqualTo(ck.columns.get(i).name);
      assertThat(get.getClusteringKey().get().getTextValue(i)).isEqualTo(ck.columns.get(i).value);
    }
  }

  @Test
  void handleKey_GivenInsert_WithNoExistingRecord_ShouldUpdateRecordsProperly()
      throws ExecutionException {
    // Arrange
    Value valueTx1 =
        new Value(
            null,
            "tx1",
            1,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "insert",
            Collections.singletonList(new Column<>("name", "user1")));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            0,
            null,
            null,
            false,
            Sets.newHashSet(valueTx1),
            Collections.emptySet(),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isFalse();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo).updateWithPrepTxId(key, currentRecord, "tx1");
    verify(replRecordRepo)
        .updateWithValues(
            key, currentRecord, "tx1", false, Collections.emptySet(), Collections.singleton("tx1"));

    ArgumentCaptor<Put> storagePutArgumentCaptor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(storagePutArgumentCaptor.capture());
    Put put = storagePutArgumentCaptor.getValue();
    assertStoragePutOperationMetadata(put, "ns.tbl", pk, ck);
    assertThat(put.getColumns().size()).isEqualTo(6);
    assertThat(put.getColumns().get("tx_id").getTextValue()).isEqualTo("tx1");
    assertThat(put.getColumns().get("tx_version").getIntValue()).isEqualTo(1);
    assertThat(put.getColumns().get("tx_state").getIntValue())
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(put.getColumns().get("tx_prepared_at").getBigIntValue())
        .isEqualTo(preparedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("tx_committed_at").getBigIntValue())
        .isEqualTo(committedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("name").getTextValue()).isEqualTo("user1");
    assertThat(put.getCondition())
        .isPresent()
        .isEqualTo(Optional.of(ConditionBuilder.putIfNotExists()));
  }

  @Test
  void handleKey_GivenInsert_WithExistingRecord_WhenDuplicatedRecordExists_ShouldNotUpdateRecords()
      throws ExecutionException {
    // Arrange
    Value valueTx1 =
        new Value(
            null,
            "tx1",
            1,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "insert",
            Collections.singletonList(new Column<>("name", "user1")));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            0,
            "tx1",
            null,
            false,
            Sets.newHashSet(valueTx1),
            Sets.newHashSet("tx1"),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isFalse();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo, never()).updateWithPrepTxId(any(), any(), any());
    verify(replRecordRepo, never())
        .updateWithValues(any(), any(), any(), anyBoolean(), any(), any());

    verify(storage, never()).put(any(Put.class));
  }

  @Test
  void
      handleKey_GivenInsert_WithExistingRecord_WhenRecordIsDeletedAfterDuplicatedInsert_ShouldNotUpdateRecords()
          throws ExecutionException {
    // Arrange
    Value valueTx1 =
        new Value(
            null,
            "tx1",
            1,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "insert",
            Collections.singletonList(new Column<>("name", "user1")));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            2,
            "tx2",
            null,
            true,
            // tx1 is retried with some delay.
            Sets.newHashSet(valueTx1),
            Sets.newHashSet("tx1"),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isFalse();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo, never()).updateWithPrepTxId(any(), any(), any());
    verify(replRecordRepo, never())
        .updateWithValues(any(), any(), any(), anyBoolean(), any(), any());

    verify(storage, never()).put(any(Put.class));
  }

  @Test
  void handleKey_GivenUpdate_WithNoExistingRecord_ShouldNotUpdateRecords()
      throws ExecutionException {
    // Arrange
    Value valueTx2 =
        new Value(
            "tx1",
            "tx2",
            2,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "update",
            Collections.singletonList(new Column<>("name", "user1")));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            0,
            null,
            null,
            false,
            Sets.newHashSet(valueTx2),
            Collections.emptySet(),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isFalse();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo, never()).updateWithPrepTxId(any(), any(), any());
    verify(replRecordRepo, never())
        .updateWithValues(any(), any(), any(), anyBoolean(), any(), any());

    verify(storage, never()).put(any(Put.class));
  }

  @Test
  void handleKey_GivenConnectedTwoUpdates_WithExistingRecord_ShouldUpdateRecordsProperly()
      throws ExecutionException {
    // Arrange
    Value valueTx2 =
        new Value(
            "tx1",
            "tx2",
            2,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)));
    Value valueTx3 =
        new Value(
            "tx2",
            "tx3",
            3,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "update",
            Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33)));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(valueTx2, valueTx3),
            Collections.emptySet(),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isFalse();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo).updateWithPrepTxId(key, currentRecord, "tx3");
    verify(replRecordRepo)
        .updateWithValues(
            key, currentRecord, "tx3", false, Collections.emptySet(), Collections.emptySet());

    ArgumentCaptor<Put> storagePutArgumentCaptor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(storagePutArgumentCaptor.capture());
    Put put = storagePutArgumentCaptor.getValue();
    assertStoragePutOperationMetadata(put, "ns.tbl", pk, ck);
    assertThat(put.getColumns().size()).isEqualTo(8);
    assertThat(put.getColumns().get("tx_id").getTextValue()).isEqualTo("tx3");
    assertThat(put.getColumns().get("tx_version").getIntValue()).isEqualTo(3);
    assertThat(put.getColumns().get("tx_state").getIntValue())
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(put.getColumns().get("tx_prepared_at").getBigIntValue())
        .isEqualTo(preparedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("tx_committed_at").getBigIntValue())
        .isEqualTo(committedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("name").getTextValue()).isEqualTo("user2");
    assertThat(put.getColumns().get("comment").getTextValue()).isEqualTo("hello");
    assertThat(put.getColumns().get("age").getIntValue()).isEqualTo(33);
    assertThat(put.getCondition())
        .isPresent()
        .isEqualTo(
            Optional.of(
                ConditionBuilder.putIf(
                        ConditionBuilder.buildConditionalExpression(
                            TextColumn.of("tx_id", "tx1"), Operator.EQ))
                    .build()));
  }

  @Test
  void
      handleKey_GivenConnectedTwoUpdates_WithExistingRecord_WhenBackupDbTableIsAlreadyUpdated_ShouldUpdateRecordsProperly()
          throws ExecutionException {
    // Arrange
    Value valueTx2 =
        new Value(
            "tx1",
            "tx2",
            2,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)));
    Value valueTx3 =
        new Value(
            "tx2",
            "tx3",
            3,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "update",
            Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33)));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(valueTx2, valueTx3),
            Collections.emptySet(),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    doThrow(NoMutationException.class).doNothing().when(storage).put(any(Put.class));
    Result result = mock(Result.class);
    // The backup DB table is already updated. Probably, updating the replication DB table failed
    // after that.
    doReturn("tx3").when(result).getText("tx_id");
    doReturn(Optional.of(result)).when(storage).get(any(Get.class));

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isFalse();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo).updateWithPrepTxId(key, currentRecord, "tx3");
    verify(replRecordRepo)
        .updateWithValues(
            key, currentRecord, "tx3", false, Collections.emptySet(), Collections.emptySet());

    ArgumentCaptor<Put> storagePutArgumentCaptor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(storagePutArgumentCaptor.capture());
    Put put = storagePutArgumentCaptor.getValue();
    assertStoragePutOperationMetadata(put, "ns.tbl", pk, ck);
    assertThat(put.getColumns().size()).isEqualTo(8);
    assertThat(put.getColumns().get("tx_id").getTextValue()).isEqualTo("tx3");
    assertThat(put.getColumns().get("tx_version").getIntValue()).isEqualTo(3);
    assertThat(put.getColumns().get("tx_state").getIntValue())
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(put.getColumns().get("tx_prepared_at").getBigIntValue())
        .isEqualTo(preparedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("tx_committed_at").getBigIntValue())
        .isEqualTo(committedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("name").getTextValue()).isEqualTo("user2");
    assertThat(put.getColumns().get("comment").getTextValue()).isEqualTo("hello");
    assertThat(put.getColumns().get("age").getIntValue()).isEqualTo(33);
    assertThat(put.getCondition())
        .isPresent()
        .isEqualTo(
            Optional.of(
                ConditionBuilder.putIf(
                        ConditionBuilder.buildConditionalExpression(
                            TextColumn.of("tx_id", "tx1"), Operator.EQ))
                    .build()));

    ArgumentCaptor<Get> storageGetArgumentCaptor = ArgumentCaptor.forClass(Get.class);
    verify(storage).get(storageGetArgumentCaptor.capture());
    Get get = storageGetArgumentCaptor.getValue();
    assertStorageGetOperationMetadata(get, "ns.tbl", pk, ck);
  }

  @Test
  void
      handleKey_GivenConnectedTwoUpdates_WithExistingRecord_WhenBackupDbTableProceedsTooMuch_ShouldThrowException()
          throws ExecutionException {
    // Arrange
    Value valueTx2 =
        new Value(
            "tx1",
            "tx2",
            2,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)));
    Value valueTx3 =
        new Value(
            "tx2",
            "tx3",
            3,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "update",
            Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33)));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(valueTx2, valueTx3),
            Collections.emptySet(),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    doThrow(NoMutationException.class).doNothing().when(storage).put(any(Put.class));
    Result result = mock(Result.class);
    // The backup DB table and the replication DB are already updated. The fetched information is
    // out of date.
    doReturn("tx10").when(result).getText("tx_id");
    doReturn(Optional.of(result)).when(storage).get(any(Get.class));

    // Act Assert
    assertThatThrownBy(() -> keyHandler.handleKey(key, true))
        .isInstanceOf(NoMutationException.class);

    // Assert
    verify(replRecordRepo).get(key);
    verify(replRecordRepo).updateWithPrepTxId(key, currentRecord, "tx3");
    verify(replRecordRepo, never())
        .updateWithValues(any(), any(), any(), anyBoolean(), any(), any());

    ArgumentCaptor<Put> storagePutArgumentCaptor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(storagePutArgumentCaptor.capture());
    Put put = storagePutArgumentCaptor.getValue();
    assertStoragePutOperationMetadata(put, "ns.tbl", pk, ck);
    assertThat(put.getColumns().size()).isEqualTo(8);
    assertThat(put.getColumns().get("tx_id").getTextValue()).isEqualTo("tx3");
    assertThat(put.getColumns().get("tx_version").getIntValue()).isEqualTo(3);
    assertThat(put.getColumns().get("tx_state").getIntValue())
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(put.getColumns().get("tx_prepared_at").getBigIntValue())
        .isEqualTo(preparedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("tx_committed_at").getBigIntValue())
        .isEqualTo(committedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("name").getTextValue()).isEqualTo("user2");
    assertThat(put.getColumns().get("comment").getTextValue()).isEqualTo("hello");
    assertThat(put.getColumns().get("age").getIntValue()).isEqualTo(33);
    assertThat(put.getCondition())
        .isPresent()
        .isEqualTo(
            Optional.of(
                ConditionBuilder.putIf(
                        ConditionBuilder.buildConditionalExpression(
                            TextColumn.of("tx_id", "tx1"), Operator.EQ))
                    .build()));

    ArgumentCaptor<Get> storageGetArgumentCaptor = ArgumentCaptor.forClass(Get.class);
    verify(storage).get(storageGetArgumentCaptor.capture());
    Get get = storageGetArgumentCaptor.getValue();
    assertStorageGetOperationMetadata(get, "ns.tbl", pk, ck);
  }

  @Test
  void handleKey_GivenDiscreteTwoUpdates_WithExistingRecord_ShouldUpdateRecordsProperly()
      throws ExecutionException {
    // Arrange
    Value valueTx2 =
        new Value(
            "tx1",
            "tx2",
            2,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "update",
            Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)));
    Value valueTx3 =
        new Value(
            "tx20",
            "tx3",
            3,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33)));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(valueTx2, valueTx3),
            Sets.newHashSet("tx1"),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isFalse();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo).updateWithPrepTxId(key, currentRecord, "tx2");
    verify(replRecordRepo)
        .updateWithValues(
            key, currentRecord, "tx2", false, Sets.newHashSet(valueTx3), Collections.emptySet());

    ArgumentCaptor<Put> storagePutArgumentCaptor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(storagePutArgumentCaptor.capture());
    Put put = storagePutArgumentCaptor.getValue();
    assertStoragePutOperationMetadata(put, "ns.tbl", pk, ck);
    assertThat(put.getColumns().size()).isEqualTo(7);
    assertThat(put.getColumns().get("tx_id").getTextValue()).isEqualTo("tx2");
    assertThat(put.getColumns().get("tx_version").getIntValue()).isEqualTo(2);
    assertThat(put.getColumns().get("tx_state").getIntValue())
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(put.getColumns().get("tx_prepared_at").getBigIntValue())
        .isEqualTo(preparedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("tx_committed_at").getBigIntValue())
        .isEqualTo(committedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("name").getTextValue()).isEqualTo("user2");
    assertThat(put.getColumns().get("age").getIntValue()).isEqualTo(22);
    assertThat(put.getCondition())
        .isPresent()
        .isEqualTo(
            Optional.of(
                ConditionBuilder.putIf(
                        ConditionBuilder.buildConditionalExpression(
                            TextColumn.of("tx_id", "tx1"), Operator.EQ))
                    .build()));
  }

  @Test
  void
      handleKey_GivenUpdatesNotConnectedToPreviousTxId_WithExistingRecord_ShouldUpdateRecordsProperly()
          throws ExecutionException {
    // Arrange
    Value valueTx2 =
        new Value(
            "tx100",
            "tx2",
            2,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)));
    Value valueTx3 =
        new Value(
            "tx2",
            "tx3",
            3,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33)));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(valueTx2, valueTx3),
            Sets.newHashSet("tx1"),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isFalse();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo, never()).updateWithPrepTxId(any(), any(), any());
    verify(replRecordRepo, never())
        .updateWithValues(any(), any(), any(), anyBoolean(), any(), any());

    verify(storage, never()).put(any(Put.class));
  }

  @Test
  void handleKey_GivenConnectedUpdateAndDelete_WithExistingRecord_ShouldUpdateRecordsProperly()
      throws ExecutionException {
    // Arrange
    Value valueTx2 =
        new Value(
            "tx1",
            "tx2",
            2,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)));
    Value valueTx3 =
        new Value(
            "tx2",
            "tx3",
            3,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "delete",
            Collections.emptyList());
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(valueTx2, valueTx3),
            Sets.newHashSet("tx1"),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isFalse();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo).updateWithPrepTxId(key, currentRecord, "tx3");
    verify(replRecordRepo)
        .updateWithValues(
            key, currentRecord, "tx3", true, Collections.emptySet(), Collections.emptySet());

    ArgumentCaptor<Put> storagePutArgumentCaptor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(storagePutArgumentCaptor.capture());
    Put put = storagePutArgumentCaptor.getValue();
    assertStoragePutOperationMetadata(put, "ns.tbl", pk, ck);
    assertThat(put.getColumns().size()).isEqualTo(5);
    assertThat(put.getColumns().get("tx_id").getTextValue()).isEqualTo("tx3");
    assertThat(put.getColumns().get("tx_version").getIntValue()).isEqualTo(3);
    assertThat(put.getColumns().get("tx_state").getIntValue())
        .isEqualTo(TransactionState.DELETED.get());
    assertThat(put.getColumns().get("tx_prepared_at").getBigIntValue())
        .isEqualTo(preparedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("tx_committed_at").getBigIntValue())
        .isEqualTo(committedAtInMillisOfLastValue);
    assertThat(put.getCondition())
        .isPresent()
        .isEqualTo(
            Optional.of(
                ConditionBuilder.putIf(
                        ConditionBuilder.buildConditionalExpression(
                            TextColumn.of("tx_id", "tx1"), Operator.EQ))
                    .build()));
  }

  @Test
  void
      handleKey_GivenConnectedTwoUpdates_WithExistingRecord_WithPrepareTxId_ShouldUpdateRecordsProperly()
          throws ExecutionException {
    // Arrange
    Value valueTx2 =
        new Value(
            "tx1",
            "tx2",
            2,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "update",
            Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)));
    Value valueTx3 =
        new Value(
            "tx2",
            "tx3",
            3,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33)));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            1,
            "tx1",
            "tx2",
            false,
            Sets.newHashSet(valueTx2, valueTx3),
            Sets.newHashSet("tx1"),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isTrue();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo, never()).updateWithPrepTxId(any(), any(), any());
    verify(replRecordRepo)
        .updateWithValues(
            key, currentRecord, "tx2", false, Sets.newHashSet(valueTx3), Collections.emptySet());

    ArgumentCaptor<Put> storagePutArgumentCaptor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(storagePutArgumentCaptor.capture());
    Put put = storagePutArgumentCaptor.getValue();
    assertStoragePutOperationMetadata(put, "ns.tbl", pk, ck);
    assertThat(put.getColumns().size()).isEqualTo(7);
    assertThat(put.getColumns().get("tx_id").getTextValue()).isEqualTo("tx2");
    assertThat(put.getColumns().get("tx_version").getIntValue()).isEqualTo(2);
    assertThat(put.getColumns().get("tx_state").getIntValue())
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(put.getColumns().get("tx_prepared_at").getBigIntValue())
        .isEqualTo(preparedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("tx_committed_at").getBigIntValue())
        .isEqualTo(committedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("name").getTextValue()).isEqualTo("user2");
    assertThat(put.getColumns().get("age").getIntValue()).isEqualTo(22);
    assertThat(put.getCondition())
        .isPresent()
        .isEqualTo(
            Optional.of(
                ConditionBuilder.putIf(
                        ConditionBuilder.buildConditionalExpression(
                            TextColumn.of("tx_id", "tx1"), Operator.EQ))
                    .build()));
  }

  @Test
  void handleKey_GivenConnectedAllTypeOperations_WithExistingRecord_ShouldUpdateRecordsProperly()
      throws ExecutionException {
    // Arrange
    Value valueTx2 =
        new Value(
            "tx1",
            "tx2",
            2,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)));
    Value valueTx3 =
        new Value(
            "tx2",
            "tx3",
            3,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "delete",
            Collections.emptyList());
    Value valueTx4 =
        new Value(
            null,
            "tx4",
            1,
            preparedAtInMillisOfLastValue,
            committedAtInMillisOfLastValue,
            "insert",
            Arrays.asList(new Column<>("name", "user11"), new Column<>("age", 111)));
    Value valueTx5 =
        new Value(
            "tx4",
            "tx5",
            2,
            System.currentTimeMillis(),
            System.currentTimeMillis(),
            "update",
            Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 222)));
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(valueTx2, valueTx3, valueTx4, valueTx5),
            Sets.newHashSet("tx1"),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isTrue();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo).updateWithPrepTxId(key, currentRecord, "tx4");
    verify(replRecordRepo)
        .updateWithValues(
            key, currentRecord, "tx4", false, Sets.newHashSet(valueTx5), Sets.newHashSet("tx4"));

    ArgumentCaptor<Put> storagePutArgumentCaptor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(storagePutArgumentCaptor.capture());
    Put put = storagePutArgumentCaptor.getValue();
    assertStoragePutOperationMetadata(put, "ns.tbl", pk, ck);
    assertThat(put.getColumns().size()).isEqualTo(7);
    assertThat(put.getColumns().get("tx_id").getTextValue()).isEqualTo("tx4");
    assertThat(put.getColumns().get("tx_version").getIntValue()).isEqualTo(1);
    assertThat(put.getColumns().get("tx_state").getIntValue())
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(put.getColumns().get("tx_prepared_at").getBigIntValue())
        .isEqualTo(preparedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("tx_committed_at").getBigIntValue())
        .isEqualTo(committedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("name").getTextValue()).isEqualTo("user11");
    assertThat(put.getColumns().get("age").getIntValue()).isEqualTo(111);
    assertThat(put.getCondition())
        .isPresent()
        .isEqualTo(
            Optional.of(
                ConditionBuilder.putIf(
                        ConditionBuilder.buildConditionalExpression(
                            TextColumn.of("tx_id", "tx1"), Operator.EQ))
                    .build()));
  }

  @Test
  void handleKey_GivenManyInserts_WithPreparedTxId_ShouldUpdateRecordsProperlyWithPreparedId()
      throws ExecutionException {
    // Arrange
    int n = 200;
    Set<Value> values = new HashSet<>();
    for (int i = 0; i < n; i++) {
      values.add(
          new Value(
              null,
              String.format("tx%05d", i),
              i,
              preparedAtInMillisOfLastValue,
              committedAtInMillisOfLastValue,
              "insert",
              Arrays.asList(
                  new Column<>("name", String.format("user%05d", i)), new Column<>("age", i))));
    }
    Random random = new Random();
    int preparedTxIdNum = random.nextInt(n);
    String preparedTxId = String.format("tx%05d", preparedTxIdNum);
    Record currentRecord =
        new Record(
            "ns",
            "tbl",
            pk,
            ck,
            100,
            "txZZZ",
            preparedTxId,
            true,
            values,
            Sets.newHashSet("txAAA"),
            null,
            null);

    doReturn(Optional.of(currentRecord)).when(replRecordRepo).get(any());

    // Act
    boolean shouldHandleTheSameKey = keyHandler.handleKey(key, true);

    // Assert
    assertThat(shouldHandleTheSameKey).isTrue();

    verify(replRecordRepo).get(key);
    verify(replRecordRepo, never()).updateWithPrepTxId(any(), any(), any());
    verify(replRecordRepo)
        .updateWithValues(
            key,
            currentRecord,
            preparedTxId,
            false,
            values.stream().filter(v -> !v.txId.equals(preparedTxId)).collect(Collectors.toSet()),
            Sets.newHashSet(preparedTxId));

    ArgumentCaptor<Put> storagePutArgumentCaptor = ArgumentCaptor.forClass(Put.class);
    verify(storage).put(storagePutArgumentCaptor.capture());
    Put put = storagePutArgumentCaptor.getValue();
    assertStoragePutOperationMetadata(put, "ns.tbl", pk, ck);
    assertThat(put.getColumns().size()).isEqualTo(7);
    assertThat(put.getColumns().get("tx_id").getTextValue()).isEqualTo(preparedTxId);
    assertThat(put.getColumns().get("tx_version").getIntValue()).isEqualTo(preparedTxIdNum);
    assertThat(put.getColumns().get("tx_state").getIntValue())
        .isEqualTo(TransactionState.COMMITTED.get());
    assertThat(put.getColumns().get("tx_prepared_at").getBigIntValue())
        .isEqualTo(preparedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("tx_committed_at").getBigIntValue())
        .isEqualTo(committedAtInMillisOfLastValue);
    assertThat(put.getColumns().get("name").getTextValue())
        .isEqualTo(String.format("user%05d", preparedTxIdNum));
    assertThat(put.getColumns().get("age").getIntValue()).isEqualTo(preparedTxIdNum);
    assertThat(put.getCondition())
        .isPresent()
        .isEqualTo(
            Optional.of(
                ConditionBuilder.putIf(
                        ConditionBuilder.buildConditionalExpression(
                            TextColumn.of("tx_id", "txZZZ"), Operator.EQ))
                    .build()));
  }
}
