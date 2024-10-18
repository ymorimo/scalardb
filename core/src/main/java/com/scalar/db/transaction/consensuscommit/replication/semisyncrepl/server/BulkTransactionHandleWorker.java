package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkTransactionHandleWorker {
  private static final Logger logger = LoggerFactory.getLogger(BulkTransactionHandleWorker.class);

  private final ReplicationBulkTransactionRepository bulkTransactionRepository;
  private final TransactionHandleWorker transactionHandleWorker;
  private final ExecutorService bulkTransactionHandlerExecutorService;
  private final Configuration conf;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration {
    final int bulkTransactionHandlerThreadSize;
    final int waitMillisPerPartition;

    public Configuration(int bulkTransactionHandlerThreadSize, int waitMillisPerPartition) {
      this.bulkTransactionHandlerThreadSize = bulkTransactionHandlerThreadSize;
      this.waitMillisPerPartition = waitMillisPerPartition;
    }
  }

  public BulkTransactionHandleWorker(
      Configuration conf,
      ReplicationBulkTransactionRepository bulkTransactionRepository,
      TransactionHandleWorker transactionHandleWorker,
      MetricsLogger metricsLogger) {

    this.conf = conf;
    this.bulkTransactionRepository = bulkTransactionRepository;
    this.transactionHandleWorker = transactionHandleWorker;
    this.bulkTransactionHandlerExecutorService =
        Executors.newFixedThreadPool(
            conf.bulkTransactionHandlerThreadSize,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("bulk-txn-handler-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.metricsLogger = metricsLogger;
  }

  private void handleBulkTransactionWithRetry(BulkTransaction bulkTransaction) {
    if (bulkTransaction.transactions.isEmpty()) {
      try {
        logger.warn("Empty transactions. BulkTransaction: {}", bulkTransaction);
        bulkTransactionRepository.delete(bulkTransaction);
      } catch (ExecutionException e) {
        // TODO
        throw new RuntimeException(e);
      }
      return;
    }

    CountDownLatch countDownLatch = new CountDownLatch(1);
    Set<Transaction> remainingTransactions =
        new ConcurrentSkipListSet<>(bulkTransaction.transactions);

    while (true) {
      try {
        for (Transaction transaction : bulkTransaction.transactions) {
          transactionHandleWorker.enqueue(
              transaction,
              () -> {
                if (!remainingTransactions.remove(transaction)) {
                  logger.warn(
                      "The Transaction {} wasn't contained in {}. Remaining transactions: {}",
                      transaction,
                      bulkTransaction,
                      remainingTransactions);
                }

                if (remainingTransactions.isEmpty()) {
                  try {
                    bulkTransactionRepository.delete(bulkTransaction);
                    countDownLatch.countDown();
                  } catch (ExecutionException e) {
                    // TODO
                    throw new RuntimeException(e);
                  }
                }
              });
        }
        countDownLatch.await();
        return;
      } catch (Exception e) {
        metricsLogger.incrementExceptionCount();
        logger.error("Failed to handle a dequeued BulkTransaction: {}", bulkTransaction, e);
      }
      // TODO: Is this needed?
      // metricsLogger.incrementRetryTransaction();
      Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
    }
  }

  public void enqueue(BulkTransaction bulkTransaction) {
    bulkTransactionHandlerExecutorService.execute(
        () -> handleBulkTransactionWithRetry(bulkTransaction));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("bulkTransactionHandlerExecutorService", bulkTransactionHandlerExecutorService)
        .toString();
  }

  public boolean isBusy() {
    return getActiveCount() >= getMaximumPoolSize();
  }

  public int getMaximumPoolSize() {
    return ((ThreadPoolExecutor) bulkTransactionHandlerExecutorService).getMaximumPoolSize();
  }

  public int getPoolSize() {
    return ((ThreadPoolExecutor) bulkTransactionHandlerExecutorService).getPoolSize();
  }

  public int getActiveCount() {
    return ((ThreadPoolExecutor) bulkTransactionHandlerExecutorService).getActiveCount();
  }

  public String toJson() {
    return String.format(
        "{\"Thread\":{\"BulkTxn\":{\"Total\":%d, \"Active\":%d}}}",
        getPoolSize(), getActiveCount());
  }
}
