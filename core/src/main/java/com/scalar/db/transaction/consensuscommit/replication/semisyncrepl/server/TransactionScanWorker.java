package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import java.util.List;
import javax.annotation.concurrent.Immutable;

public class TransactionScanWorker extends BaseScanWorker {
  private final Configuration conf;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final MetricsLogger metricsLogger;
  private final TransactionHandleWorker transactionHandleWorker;

  @Immutable
  public static class Configuration extends BaseScanWorker.Configuration {
    private final int fetchSize;

    public Configuration(
        int replicationDbPartitionSize, int threadSize, int waitMillisPerPartition, int fetchSize) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
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
  }

  private void moveTransaction(Transaction transaction) throws ExecutionException {
    metricsLogger.incrTxnsScannedTxns();
    replicationTransactionRepository.add(transaction);
    transactionHandleWorker.enqueue(transaction);
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    List<Transaction> scannedTxns =
        metricsLogger.execScanTransactions(
            () -> replicationTransactionRepository.scan(partitionId, conf.fetchSize));
    for (Transaction transaction : scannedTxns) {
      moveTransaction(transaction);
    }

    return scannedTxns.size() >= conf.fetchSize;
  }
}
