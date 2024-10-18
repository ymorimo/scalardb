package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.BulkTransaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationBulkTransactionRepository.ScanResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkTransactionScanWorker extends BaseScanWorker {
  private static final Logger logger = LoggerFactory.getLogger(BulkTransactionScanWorker.class);
  private final Configuration conf;
  private final ReplicationBulkTransactionRepository replicationBulkTransactionRepository;
  private final MetricsLogger metricsLogger;
  private final BulkTransactionHandleWorker bulkTransactionHandleWorker;
  private final Map<Integer, Long> lastScannedTimestampMap = new HashMap<>();

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
      BulkTransactionHandleWorker bulkTransactionHandleWorker,
      MetricsLogger metricsLogger) {
    super(conf, "bulk-tx", metricsLogger);
    this.conf = conf;
    this.replicationBulkTransactionRepository = replicationBulkTransactionRepository;
    this.bulkTransactionHandleWorker = bulkTransactionHandleWorker;
    this.metricsLogger = metricsLogger;
  }

  @Override
  protected boolean handle(int partitionId) throws ExecutionException {
    // If the worker is busy, wait for a while.
    if (bulkTransactionHandleWorker.getActiveCount()
        >= bulkTransactionHandleWorker.getMaximumPoolSize()) {
      return false;
    }

    @Nullable Long scanStartTsMillis = lastScannedTimestampMap.get(partitionId);

    ScanResult scanResult =
        metricsLogger.execScanBulkTransactions(
            () ->
                replicationBulkTransactionRepository.scan(
                    partitionId, conf.fetchSize, scanStartTsMillis));
    List<BulkTransaction> scannedBulkTxns = scanResult.bulkTransactions;
    metricsLogger.incrBlkTxnsScannedTxns(scannedBulkTxns.size());
    if (scannedBulkTxns.isEmpty()) {
      lastScannedTimestampMap.remove(partitionId);
      return false;
    }

    for (BulkTransaction bulkTransaction : scannedBulkTxns) {
      bulkTransactionHandleWorker.enqueue(bulkTransaction);
    }

    if (scanResult.nextScanTimestampMillis == null || scannedBulkTxns.size() < conf.fetchSize) {
      // It's likely no more record is stored.
      lastScannedTimestampMap.remove(partitionId);
    } else {
      lastScannedTimestampMap.put(partitionId, scanResult.nextScanTimestampMillis);
    }

    return scannedBulkTxns.size() >= conf.fetchSize;
  }
}
