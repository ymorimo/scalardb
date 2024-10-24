package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseScanWorker {
  private static final Logger logger = LoggerFactory.getLogger(BaseScanWorker.class);

  private final ExecutorService executorService;
  private final Configuration conf;
  private final MetricsLogger metricsLogger;

  @Immutable
  public static class Configuration {
    final int replicationDbPartitionSize;
    final int threadSize;
    final int waitMillisPerPartition;

    public Configuration(
        int replicationDbPartitionSize, int threadSize, int waitMillisPerPartition) {
      this.replicationDbPartitionSize = replicationDbPartitionSize;
      this.threadSize = threadSize;
      this.waitMillisPerPartition = waitMillisPerPartition;
    }
  }

  public BaseScanWorker(Configuration conf, String label, MetricsLogger metricsLogger) {
    if (conf.replicationDbPartitionSize % conf.threadSize != 0) {
      throw new IllegalArgumentException(
          String.format(
              "`replicationDbPartitionSize`(%d) should be a multiple of `replicationDbThreadSize`(%d)",
              conf.replicationDbPartitionSize, conf.threadSize));
    }
    this.conf = conf;

    this.executorService =
        Executors.newFixedThreadPool(
            conf.threadSize,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(String.format("%s-scan", label) + "-%d")
                .setUncaughtExceptionHandler(
                    (thread, e) -> logger.error("Got an uncaught exception. thread:{}", thread, e))
                .build());
    this.metricsLogger = metricsLogger;
  }

  protected abstract boolean handle(int partitionId) throws Exception;

  protected boolean shouldFinish() {
    return false;
  }

  private void waitAndUpdateNextInvocations(Long[] nextInvocationsMillis) {
    long currentTimeMillis = System.currentTimeMillis();
    long soonestNextInvocationMillis = Long.MAX_VALUE;
    for (int i = 0; i < nextInvocationsMillis.length; i++) {
      if (nextInvocationsMillis[i] != null && nextInvocationsMillis[i] <= currentTimeMillis) {
        nextInvocationsMillis[i] = null;
      }
      if (nextInvocationsMillis[i] == null) {
        // TODO: This should return boolean to tell if it's possible to continue processing the
        // partition.
        // If any partition can be immediately handled, no wait is needed.
        return;
      }
      soonestNextInvocationMillis = Math.min(soonestNextInvocationMillis, nextInvocationsMillis[i]);
    }

    // If all partitions should be handled later, let's wait for the soonest timing.
    long durationForSoonestPartitionMills = soonestNextInvocationMillis - currentTimeMillis;
    metricsLogger.execWaitBulkTransactions(
        () -> {
          Uninterruptibles.sleepUninterruptibly(
              Duration.ofMillis(durationForSoonestPartitionMills));
          return null;
        });
  }

  private void execLoop(int startPartitionId) {
    int partitionSizePerThread = conf.replicationDbPartitionSize / conf.threadSize;
    Long[] nextInvocationsMillis = new Long[partitionSizePerThread];
    while (!shouldFinish()) {
      for (int partitionIndex = 0; partitionIndex < partitionSizePerThread; partitionIndex++) {
        int partitionId = startPartitionId + (conf.threadSize * partitionIndex);

        try {
          if (nextInvocationsMillis[partitionIndex] != null) {
            waitAndUpdateNextInvocations(nextInvocationsMillis);
            continue;
          }

          if (handle(partitionId)) {
            // Fetched full size entries. Next scan should be executed immediately.
            nextInvocationsMillis[partitionIndex] = null;
          } else {
            // Fetched partial size entries. Next scan should be delayed.
            nextInvocationsMillis[partitionIndex] =
                System.currentTimeMillis() + conf.waitMillisPerPartition;
          }
        } catch (Throwable e) {
          metricsLogger.incrementExceptionCount();
          logger.error("Unexpected exception occurred", e);
        }
      }
    }
  }

  public void run() {
    for (int threadIndex = 0; threadIndex < conf.threadSize; threadIndex++) {
      int startPartitionId = threadIndex;
      executorService.execute(() -> execLoop(startPartitionId));
    }
  }
}
