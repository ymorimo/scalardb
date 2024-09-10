package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.WrittenTuple;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ReplicationTransactionRepository {

  private final TypeReference<List<WrittenTuple>> typeReferenceForWrittenTuples =
      new TypeReference<List<WrittenTuple>>() {};

  private final DistributedStorage replicationDbStorage;
  private final String replicationDbNamespace;
  private final String replicationDbTransactionTable;

  public ReplicationTransactionRepository(
      DistributedStorage replicationDbStorage,
      String replicationDbNamespace,
      String replicationDbTransactionTable) {
    this.replicationDbStorage = replicationDbStorage;
    this.replicationDbNamespace = replicationDbNamespace;
    this.replicationDbTransactionTable = replicationDbTransactionTable;
  }

  public static class ScanResult {
    public final Long nextScanTimestampMillis;
    public final List<Transaction> transactions;

    public ScanResult(Long nextScanTimestampMillis, List<Transaction> transactions) {
      this.nextScanTimestampMillis = nextScanTimestampMillis;
      this.transactions = transactions;
    }
  }

  public ScanResult scan(int partitionId, int fetchTransactionSize, long scanStartTsMillis)
      throws ExecutionException, IOException {
    try (Scanner scan =
        replicationDbStorage.scan(
            Scan.newBuilder()
                .namespace(replicationDbNamespace)
                .table(replicationDbTransactionTable)
                .partitionKey(Key.ofInt("partition_id", partitionId))
                .ordering(Ordering.asc("created_at"))
                .end(Key.ofBigInt("created_at", scanStartTsMillis))
                .limit(fetchTransactionSize)
                .build())) {

      Long lastTimestampMillis = null;
      List<Transaction> transactions = new ArrayList<>(scan.all().size());
      for (Result result : scan.all()) {
        String transactionId = result.getText("transaction_id");
        Instant createdAt = Instant.ofEpochMilli(result.getBigInt("created_at"));
        List<WrittenTuple> writtenTuples;
        String writeSet = result.getText("write_set");
        writtenTuples = JSON.parseObject(writeSet, typeReferenceForWrittenTuples);
        if (lastTimestampMillis == null || lastTimestampMillis < createdAt.toEpochMilli()) {
          lastTimestampMillis = createdAt.toEpochMilli();
        }
        transactions.add(new Transaction(partitionId, createdAt, transactionId, writtenTuples));
      }

      return new ScanResult(lastTimestampMillis, transactions);
    }
  }

  private Put createPutFromTransaction(Transaction transaction) {
    String writeSet = JSON.toJSONString(transaction.writtenTuples);

    return Put.newBuilder()
        .namespace(replicationDbNamespace)
        .table(replicationDbTransactionTable)
        .partitionKey(Key.ofInt("partition_id", transaction.partitionId))
        .clusteringKey(
            Key.newBuilder()
                .addBigInt("created_at", transaction.createdAt.toEpochMilli())
                .addText("transaction_id", transaction.transactionId)
                .build())
        .value(TextColumn.of("write_set", writeSet))
        .build();
  }

  public void add(Transaction transaction) throws ExecutionException {
    replicationDbStorage.put(createPutFromTransaction(transaction));
  }

  public Transaction updateCreatedAt(Transaction transaction) throws ExecutionException {
    Instant now = Instant.now();
    Transaction updatedTransaction =
        new Transaction(
            transaction.partitionId, now, transaction.transactionId, transaction.writtenTuples);

    add(updatedTransaction);
    // It's okay if deleting the old record remains
    delete(transaction);

    return updatedTransaction;
  }

  public void delete(Transaction transaction) throws ExecutionException {
    replicationDbStorage.delete(
        Delete.newBuilder()
            .namespace(replicationDbNamespace)
            .table(replicationDbTransactionTable)
            .partitionKey(Key.ofInt("partition_id", transaction.partitionId))
            .clusteringKey(
                Key.newBuilder()
                    .addBigInt("created_at", transaction.createdAt.toEpochMilli())
                    .addText("transaction_id", transaction.transactionId)
                    .build())
            .build());
  }
}
