package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Create `BaseQueueConsumer` and inherit it.
public class TransactionQueueConsumer {
  private static final Logger logger = LoggerFactory.getLogger(TransactionQueueConsumer.class);

  private final TransactionHandler transactionHandler;
  private final ExecutorService executorService;
  private final Configuration conf;
  private final BlockingQueue<Transaction> queue;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration {
    final int threadSize;
    final int waitMillisPerPartition;

    public Configuration(int threadSize, int waitMillisPerPartition) {
      this.threadSize = threadSize;
      this.waitMillisPerPartition = waitMillisPerPartition;
    }
  }

  public TransactionQueueConsumer(
      Configuration conf,
      TransactionHandler transactionHandler,
      BlockingQueue<Transaction> queue,
      MetricsLogger metricsLogger) {

    this.conf = conf;
    this.transactionHandler = transactionHandler;
    this.executorService =
        Executors.newFixedThreadPool(
            conf.threadSize,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("log-distributor-transaction-queue-consume-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.queue = queue;
    this.metricsLogger = metricsLogger;
  }

  public void run() {
    for (int i = 0; i < conf.threadSize; i++) {
      int threadId = i;
      executorService.execute(
          () -> {
            while (true) {
              Transaction transaction = null;
              try {
                metricsLogger.incrementDequeueFromTransactionQueue();
                transaction = queue.take();
                Optional<Transaction> updatedTransaction =
                    transactionHandler.handleTransaction(transaction);
                if (updatedTransaction.isPresent()) {
                  metricsLogger.incrementReEnqueueFromTransactionQueue();
                  queue.add(transaction);
                }
              } catch (InterruptedException e) {
                // TODO: Error handling.
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              } catch (Exception e) {
                logger.error("Failed to handle a dequeued Transaction: {}", transaction, e);
                if (transaction != null) {
                  metricsLogger.incrementReEnqueueFromTransactionQueue();
                  queue.add(transaction);
                }
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(200));
              }
            }
          });
    }
  }
}
