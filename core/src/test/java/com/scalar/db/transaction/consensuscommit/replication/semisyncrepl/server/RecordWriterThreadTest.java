package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;
import com.google.errorprone.annotations.concurrent.LazyInit;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.RecordWriterThread.KeyHandler;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.RecordWriterThread.KeyHandler.NextValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RecordWriterThreadTest {
  @Mock private ReplicationRecordRepository replRecordRepo;
  @Mock private DistributedStorage storage;
  @LazyInit private MetricsLogger metricsLogger;
  @LazyInit private KeyHandler keyHandler;

  @BeforeEach
  void setUp() {
    metricsLogger = new MetricsLogger(new LinkedBlockingQueue<>());
    keyHandler = new KeyHandler(replRecordRepo, storage, metricsLogger);
  }

  @Test
  void findNextValue_GivenInsert_WithNoExistingRecord_ShouldProcessAllValuesAndReturnNextValue() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            0,
            null,
            null,
            false,
            Sets.newHashSet(
                new Value(
                    null,
                    "tx1",
                    1,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "insert",
                    Collections.singletonList(new Column<>("name", "user1")))),
            Collections.emptySet(),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx1");
    assertThat(nextValue.nextValue.prevTxId).isNull();
    assertThat(nextValue.nextValue.txVersion).isEqualTo(1);
    assertThat(nextValue.nextValue.type).isEqualTo("insert");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns.size()).isEqualTo(1);
    assertThat(nextValue.nextValue.columns)
        .containsExactlyInAnyOrder(new Column<>("name", "user1"));
    assertThat(nextValue.deleted).isFalse();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1");
    assertThat(nextValue.restValues).isEmpty();
    assertThat(nextValue.shouldHandleTheSameKey).isFalse();
    assertThat(nextValue.updatedColumns).containsExactlyInAnyOrder(new Column<>("name", "user1"));
  }

  @Test
  void findNextValue_GivenUpdate_WithNoExistingRecord_ShouldReturnNull() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            0,
            null,
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Collections.singletonList(new Column<>("name", "user1")))),
            Collections.emptySet(),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNull();
  }

  @Test
  void
      findNextValue_GivenConnectedTwoUpdates_WithExistingRecord_ShouldProcessAllValuesAndReturnNextValue() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33))),
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx3");
    assertThat(nextValue.nextValue.prevTxId).isEqualTo("tx2");
    assertThat(nextValue.nextValue.txVersion).isEqualTo(3);
    assertThat(nextValue.nextValue.type).isEqualTo("update");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns.size()).isEqualTo(2);
    assertThat(nextValue.nextValue.columns)
        .containsExactlyInAnyOrder(new Column<>("comment", "hello"), new Column<>("age", 33));
    assertThat(nextValue.deleted).isFalse();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1");
    assertThat(nextValue.restValues).isEmpty();
    assertThat(nextValue.shouldHandleTheSameKey).isFalse();
    assertThat(nextValue.updatedColumns)
        .containsExactlyInAnyOrder(
            new Column<>("name", "user2"),
            new Column<>("comment", "hello"),
            new Column<>("age", 33));
  }

  @Test
  void
      findNextValue_GivenDiscreteTwoUpdates_WithExistingRecord_ShouldProcessOnlyOneValueAndReturnNextValue() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx20",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33))),
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx2");
    assertThat(nextValue.nextValue.prevTxId).isEqualTo("tx1");
    assertThat(nextValue.nextValue.txVersion).isEqualTo(2);
    assertThat(nextValue.nextValue.type).isEqualTo("update");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns.size()).isEqualTo(2);
    assertThat(nextValue.nextValue.columns)
        .containsExactlyInAnyOrder(new Column<>("name", "user2"), new Column<>("age", 22));
    assertThat(nextValue.deleted).isFalse();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1");
    assertThat(nextValue.restValues.stream().map(v -> v.txId)).containsExactlyInAnyOrder("tx3");
    assertThat(nextValue.shouldHandleTheSameKey).isFalse();
    assertThat(nextValue.updatedColumns)
        .containsExactlyInAnyOrder(new Column<>("name", "user2"), new Column<>("age", 22));
  }

  @Test
  void findNextValue_GivenUpdatesNotConnectedToPreviousTxId_WithExistingRecord_ShouldReturnNull() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33))),
                new Value(
                    "tx100",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNull();
  }

  @Test
  void
      findNextValue_GivenConnectedUpdateAndDelete_WithExistingRecord_ShouldProcessAllValuesAndReturnNextValue() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "delete",
                    Collections.emptyList()),
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx3");
    assertThat(nextValue.nextValue.prevTxId).isEqualTo("tx2");
    assertThat(nextValue.nextValue.txVersion).isEqualTo(3);
    assertThat(nextValue.nextValue.type).isEqualTo("delete");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns).isEmpty();
    assertThat(nextValue.deleted).isTrue();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1");
    assertThat(nextValue.restValues).isEmpty();
    assertThat(nextValue.shouldHandleTheSameKey).isFalse();
    assertThat(nextValue.updatedColumns).isEmpty();
  }

  @Test
  void
      findNextValue_GivenConnectedTwoUpdates_WithExistingRecord_WithPrepareTxId_ShouldStopAtPreparedTxIdAndReturnNextValue() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            "tx2",
            false,
            Sets.newHashSet(
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33))),
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx2");
    assertThat(nextValue.nextValue.prevTxId).isEqualTo("tx1");
    assertThat(nextValue.nextValue.txVersion).isEqualTo(2);
    assertThat(nextValue.nextValue.type).isEqualTo("update");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns.size()).isEqualTo(2);
    assertThat(nextValue.nextValue.columns)
        .containsExactlyInAnyOrder(new Column<>("name", "user2"), new Column<>("age", 22));
    assertThat(nextValue.deleted).isFalse();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1");
    assertThat(nextValue.restValues.stream().map(v -> v.txId)).containsExactlyInAnyOrder("tx3");
    assertThat(nextValue.shouldHandleTheSameKey).isTrue();
    assertThat(nextValue.updatedColumns)
        .containsExactlyInAnyOrder(new Column<>("name", "user2"), new Column<>("age", 22));
  }

  @Test
  void
      findNextValue_GivenConnectedAllTypeOperations_WithExistingRecord_ShouldStopAfterInsertAndReturnNextValue() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22))),
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "delete",
                    Collections.emptyList()),
                new Value(
                    null,
                    "tx4",
                    1,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "insert",
                    Arrays.asList(new Column<>("name", "user11"), new Column<>("age", 111))),
                new Value(
                    "tx4",
                    "tx5",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 222)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx4");
    assertThat(nextValue.nextValue.prevTxId).isNull();
    assertThat(nextValue.nextValue.txVersion).isEqualTo(1);
    assertThat(nextValue.nextValue.type).isEqualTo("insert");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns.size()).isEqualTo(2);
    assertThat(nextValue.nextValue.columns)
        .containsExactlyInAnyOrder(new Column<>("name", "user111"), new Column<>("age", 111));
    assertThat(nextValue.deleted).isFalse();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1", "tx4");
    assertThat(nextValue.restValues.stream().map(v -> v.txId)).containsExactlyInAnyOrder("tx5");
    assertThat(nextValue.shouldHandleTheSameKey).isTrue();
    assertThat(nextValue.updatedColumns)
        .containsExactlyInAnyOrder(new Column<>("name", "user111"), new Column<>("age", 111));
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

  @Test
  void handleKey_GivenInsert_WithNoExistingRecord_ShouldUpdateRecordsProperly()
      throws ExecutionException {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Key pk = new Key(new Column<>("pk", "pk1"));
    Key ck = new Key(new Column<>("ck", "ck1"));
    long preparedAtInMillisOfLastValue = System.currentTimeMillis() - 400;
    long committedAtInMillisOfLastValue = System.currentTimeMillis() - 200;
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
            Sets.newHashSet(
                new Value(
                    null,
                    "tx1",
                    1,
                    preparedAtInMillisOfLastValue,
                    committedAtInMillisOfLastValue,
                    "insert",
                    Collections.singletonList(new Column<>("name", "user1")))),
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
    assertThat(put.getColumns().get("tx_state").getIntValue()).isEqualTo(3);
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
  void handleKey_GivenUpdate_WithNoExistingRecord_ShouldDoNothing() throws ExecutionException {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Key pk = new Key(new Column<>("pk", "pk1"));
    Key ck = new Key(new Column<>("ck", "ck1"));
    long preparedAtInMillisOfLastValue = System.currentTimeMillis() - 400;
    long committedAtInMillisOfLastValue = System.currentTimeMillis() - 200;
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
            Sets.newHashSet(
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    preparedAtInMillisOfLastValue,
                    committedAtInMillisOfLastValue,
                    "update",
                    Collections.singletonList(new Column<>("name", "user1")))),
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
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Key pk = new Key(new Column<>("pk", "pk1"));
    Key ck = new Key(new Column<>("ck", "ck1"));
    long preparedAtInMillisOfLastValue = System.currentTimeMillis() - 400;
    long committedAtInMillisOfLastValue = System.currentTimeMillis() - 200;
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
            Sets.newHashSet(
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    preparedAtInMillisOfLastValue,
                    committedAtInMillisOfLastValue,
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33))),
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
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
    assertThat(put.getColumns().get("tx_state").getIntValue()).isEqualTo(3);
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
  void handleKey_GivenDiscreteTwoUpdates_WithExistingRecord_ShouldUpdateRecordsProperly() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx20",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33))),
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx2");
    assertThat(nextValue.nextValue.prevTxId).isEqualTo("tx1");
    assertThat(nextValue.nextValue.txVersion).isEqualTo(2);
    assertThat(nextValue.nextValue.type).isEqualTo("update");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns.size()).isEqualTo(2);
    assertThat(nextValue.nextValue.columns)
        .containsExactlyInAnyOrder(new Column<>("name", "user2"), new Column<>("age", 22));
    assertThat(nextValue.deleted).isFalse();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1");
    assertThat(nextValue.restValues.stream().map(v -> v.txId)).containsExactlyInAnyOrder("tx3");
    assertThat(nextValue.shouldHandleTheSameKey).isFalse();
    assertThat(nextValue.updatedColumns)
        .containsExactlyInAnyOrder(new Column<>("name", "user2"), new Column<>("age", 22));
  }

  @Test
  void
      handleKey_GivenUpdatesNotConnectedToPreviousTxId_WithExistingRecord_ShouldUpdateRecordsProperly() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33))),
                new Value(
                    "tx100",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNull();
  }

  @Test
  void handleKey_GivenConnectedUpdateAndDelete_WithExistingRecord_ShouldUpdateRecordsProperly() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "delete",
                    Collections.emptyList()),
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx3");
    assertThat(nextValue.nextValue.prevTxId).isEqualTo("tx2");
    assertThat(nextValue.nextValue.txVersion).isEqualTo(3);
    assertThat(nextValue.nextValue.type).isEqualTo("delete");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns).isEmpty();
    assertThat(nextValue.deleted).isTrue();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1");
    assertThat(nextValue.restValues).isEmpty();
    assertThat(nextValue.shouldHandleTheSameKey).isFalse();
    assertThat(nextValue.updatedColumns).isEmpty();
  }

  @Test
  void
      handleKey_GivenConnectedTwoUpdates_WithExistingRecord_WithPrepareTxId_ShouldUpdateRecordsProperly() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            "tx2",
            false,
            Sets.newHashSet(
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 33))),
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx2");
    assertThat(nextValue.nextValue.prevTxId).isEqualTo("tx1");
    assertThat(nextValue.nextValue.txVersion).isEqualTo(2);
    assertThat(nextValue.nextValue.type).isEqualTo("update");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns.size()).isEqualTo(2);
    assertThat(nextValue.nextValue.columns)
        .containsExactlyInAnyOrder(new Column<>("name", "user2"), new Column<>("age", 22));
    assertThat(nextValue.deleted).isFalse();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1");
    assertThat(nextValue.restValues.stream().map(v -> v.txId)).containsExactlyInAnyOrder("tx3");
    assertThat(nextValue.shouldHandleTheSameKey).isTrue();
    assertThat(nextValue.updatedColumns)
        .containsExactlyInAnyOrder(new Column<>("name", "user2"), new Column<>("age", 22));
  }

  @Test
  void handleKey_GivenConnectedAllTypeOperations_WithExistingRecord_ShouldUpdateRecordsProperly() {
    // Arrange
    com.scalar.db.io.Key key = com.scalar.db.io.Key.ofInt("key", 42);
    Record record =
        new Record(
            "ns",
            "tbl",
            new Key(new Column<>("pk", "pk1")),
            new Key(new Column<>("ck", "ck1")),
            1,
            "tx1",
            null,
            false,
            Sets.newHashSet(
                new Value(
                    "tx1",
                    "tx2",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("name", "user2"), new Column<>("age", 22))),
                new Value(
                    "tx2",
                    "tx3",
                    3,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "delete",
                    Collections.emptyList()),
                new Value(
                    null,
                    "tx4",
                    1,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "insert",
                    Arrays.asList(new Column<>("name", "user11"), new Column<>("age", 111))),
                new Value(
                    "tx4",
                    "tx5",
                    2,
                    System.currentTimeMillis(),
                    System.currentTimeMillis(),
                    "update",
                    Arrays.asList(new Column<>("comment", "hello"), new Column<>("age", 222)))),
            Sets.newHashSet("tx1"),
            null,
            null);

    // Act
    NextValue nextValue = keyHandler.findNextValue(key, record);

    // Assert
    assertThat(nextValue).isNotNull();
    assertThat(nextValue.nextValue.txId).isEqualTo("tx4");
    assertThat(nextValue.nextValue.prevTxId).isNull();
    assertThat(nextValue.nextValue.txVersion).isEqualTo(1);
    assertThat(nextValue.nextValue.type).isEqualTo("insert");
    // TODO: This isn't used actually.
    assertThat(nextValue.nextValue.columns.size()).isEqualTo(2);
    assertThat(nextValue.nextValue.columns)
        .containsExactlyInAnyOrder(new Column<>("name", "user111"), new Column<>("age", 111));
    assertThat(nextValue.deleted).isFalse();
    assertThat(nextValue.insertTxIds).containsExactlyInAnyOrder("tx1", "tx4");
    assertThat(nextValue.restValues.stream().map(v -> v.txId)).containsExactlyInAnyOrder("tx5");
    assertThat(nextValue.shouldHandleTheSameKey).isTrue();
    assertThat(nextValue.updatedColumns)
        .containsExactlyInAnyOrder(new Column<>("name", "user111"), new Column<>("age", 111));
  }
}
