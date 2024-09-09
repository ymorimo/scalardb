package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import static com.google.common.base.Preconditions.checkArgument;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.Immutable;

public class TransactionScanWorker extends BaseScanWorker {
  private final Configuration conf;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final MetricsLogger metricsLogger;
  private final TransactionHandleWorker transactionHandleWorker;
  private final Map<Integer, Long> lastScannedTimestampMap = new HashMap<>();

  @Immutable
  public static class Configuration extends BaseScanWorker.Configuration {
    private final int fetchSize;
    private final long transactionScanOldTimestampThresholdMillis;

    public Configuration(
        int replicationDbPartitionSize,
        int threadSize,
        int waitMillisPerPartition,
        int fetchSize,
        long transactionScanOldTimestampThresholdMillis) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
      checkArgument(fetchSize > 0, "fetch size should be greater than 0", fetchSize);
      this.fetchSize = fetchSize;
      this.transactionScanOldTimestampThresholdMillis = transactionScanOldTimestampThresholdMillis;
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
  }

  private void moveTransaction(Transaction transaction) throws ExecutionException {
    metricsLogger.incrTxnsScannedTxns();
    replicationTransactionRepository.add(transaction);
    transactionHandleWorker.enqueue(transaction);
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    Long scanStartTsMillis =
        lastScannedTimestampMap.computeIfAbsent(
            partitionId,
            key -> System.currentTimeMillis() - conf.transactionScanOldTimestampThresholdMillis);

    List<Transaction> scannedTxns =
        metricsLogger.execScanTransactions(
            () ->
                replicationTransactionRepository.scan(
                    partitionId, conf.fetchSize, scanStartTsMillis));
    for (Transaction transaction : scannedTxns) {
      moveTransaction(transaction);
    }

    if (scannedTxns.size() >= conf.fetchSize) {
      Transaction lastTransaction = scannedTxns.get(scannedTxns.size() - 1);
      // TODO: Which column should be used is hidden in ReplicationTransactionRepository.
      lastScannedTimestampMap.putIfAbsent(partitionId, lastTransaction.updatedAt.toEpochMilli());
    } else {
      lastScannedTimestampMap.put(
          partitionId,
          System.currentTimeMillis() - conf.transactionScanOldTimestampThresholdMillis);
    }

    return scannedTxns.size() >= conf.fetchSize;
  }
}
