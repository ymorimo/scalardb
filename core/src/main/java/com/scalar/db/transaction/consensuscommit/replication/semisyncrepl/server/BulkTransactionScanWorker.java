package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkTransactionScanWorker extends BaseScanWorker {
  private static final Logger logger = LoggerFactory.getLogger(BulkTransactionScanWorker.class);
  private final Configuration conf;
  private final ReplicationBulkTransactionRepository replicationBulkTransactionRepository;
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

  public BulkTransactionScanWorker(
      Configuration conf,
      ReplicationBulkTransactionRepository replicationBulkTransactionRepository,
      TransactionHandleWorker transactionHandleWorker,
      MetricsLogger metricsLogger) {
    super(conf, "bulk-tx", metricsLogger);
    this.conf = conf;
    this.replicationBulkTransactionRepository = replicationBulkTransactionRepository;
    this.transactionHandleWorker = transactionHandleWorker;
    this.metricsLogger = metricsLogger;
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    List<BulkTransaction> scannedBulkTxns =
        metricsLogger.execScanBulkTransactions(
            () -> replicationBulkTransactionRepository.scan(partitionId, conf.fetchSize));
    CountDownLatch countDownLatch = new CountDownLatch(1);
    Map<BulkTransaction, Set<Transaction>> remainingBulkTxns = new ConcurrentHashMap<>();
    for (BulkTransaction bulkTransaction : scannedBulkTxns) {
      remainingBulkTxns.put(
          bulkTransaction, new ConcurrentSkipListSet<>(bulkTransaction.transactions));
      for (Transaction transaction : bulkTransaction.transactions) {
        transactionHandleWorker.enqueue(
            transaction,
            () -> {
              Set<Transaction> remainingTxns = remainingBulkTxns.get(bulkTransaction);
              if (!remainingTxns.remove(transaction)) {
                logger.warn(
                    "The Transaction {} wasn't contained in {}", transaction, bulkTransaction);
              }
              if (remainingTxns.isEmpty()) {
                try {
                  replicationBulkTransactionRepository.delete(bulkTransaction);
                } catch (ExecutionException e) {
                  // TODO
                  throw new RuntimeException(e);
                }
                remainingBulkTxns.remove(bulkTransaction);
                if (remainingBulkTxns.isEmpty()) {
                  countDownLatch.countDown();
                }
              }
            });
      }
    }
    Uninterruptibles.awaitUninterruptibly(countDownLatch);

    return scannedBulkTxns.size() >= conf.fetchSize;
  }
}
