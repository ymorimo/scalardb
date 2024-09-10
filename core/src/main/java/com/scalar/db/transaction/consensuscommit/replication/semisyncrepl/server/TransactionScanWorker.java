package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import static com.google.common.base.Preconditions.checkArgument;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository.ScanResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FIXME: This worker must be executed only when a process starts to avoid repeatedly scanning and
// enqueueing
//        Therefore, the following changes are needed:
//        - Make BaseScanWorker able to stop scanning after `handle()` returns a specific value.
//        - TransactionScanWorker starts scanning from the oldest transaction record.
//        - TransactionScanWorker ends scanning when it handles a transaction enqueued after the
//          process started.
public class TransactionScanWorker extends BaseScanWorker {
  private static final Logger logger = LoggerFactory.getLogger(TransactionScanWorker.class);
  private final Configuration conf;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final MetricsLogger metricsLogger;
  private final TransactionHandleWorker transactionHandleWorker;
  private final Map<Integer, Long> lastScannedTimestampMap = new HashMap<>();
  private final long startTimestampMillis;
  private final Set<Integer> finishedPartitionIds;

  @Immutable
  public static class Configuration extends BaseScanWorker.Configuration {
    private final int fetchSize;

    public Configuration(
        int replicationDbPartitionSize, int threadSize, int waitMillisPerPartition, int fetchSize) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
      checkArgument(fetchSize > 0, "fetch size should be greater than 0", fetchSize);
      this.fetchSize = fetchSize;
    }
  }

  public TransactionScanWorker(
      Configuration conf,
      ReplicationTransactionRepository replicationTransactionRepository,
      TransactionHandleWorker transactionHandleWorker,
      MetricsLogger metricsLogger) {
    super(conf, "tx", metricsLogger);
    this.conf = conf;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.transactionHandleWorker = transactionHandleWorker;
    this.metricsLogger = metricsLogger;
    this.startTimestampMillis = System.currentTimeMillis();
    this.finishedPartitionIds = new HashSet<>(conf.replicationDbPartitionSize);
  }

  private void moveTransaction(Transaction transaction) throws ExecutionException {
    metricsLogger.incrTxnsScannedTxns();
    replicationTransactionRepository.add(transaction);
    transactionHandleWorker.enqueue(transaction);
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    Long scanStartTsMillis =
        lastScannedTimestampMap.computeIfAbsent(partitionId, key -> startTimestampMillis);

    ScanResult scanResult =
        metricsLogger.execScanTransactions(
            () ->
                replicationTransactionRepository.scan(
                    partitionId, conf.fetchSize, scanStartTsMillis));
    for (Transaction transaction : scanResult.transactions) {
      moveTransaction(transaction);
    }

    if (scanResult.nextScanTimestampMillis == null) {
      assert scanResult.transactions.isEmpty();
      // All transactions in Replication DB are scanned.
      lastScannedTimestampMap.remove(partitionId);
      finishedPartitionIds.add(partitionId);
      logger.info("Finishing the partition. Partition ID: {}", partitionId);
    } else {
      lastScannedTimestampMap.put(partitionId, scanResult.nextScanTimestampMillis);
    }

    return scanResult.transactions.size() >= conf.fetchSize;
  }

  @Override
  protected boolean shouldFinish() {
    return finishedPartitionIds.size() >= conf.replicationDbPartitionSize;
  }
}
