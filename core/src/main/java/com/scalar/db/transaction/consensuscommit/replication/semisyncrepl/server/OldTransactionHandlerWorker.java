package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.CoordinatorState;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.DeletedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.InsertedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Transaction;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.UpdatedTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.WrittenTuple;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.CoordinatorStateRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationTransactionRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.RecordHandler.ResultOfKeyHandling;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.function.FailableRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OldTransactionHandlerWorker extends BaseHandlerWorker {
  private static final Logger logger = LoggerFactory.getLogger(OldTransactionHandlerWorker.class);

  private final OldTransactionHandlerWorker.Configuration conf;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final MetricsLogger metricsLogger;
  private final RecordHandler recordHandler;
  private final ExecutorService recordHandlerExecutorService;

  @Immutable
  public static class Configuration extends BaseHandlerWorker.Configuration {
    private final int fetchSize;
    private final long thresholdMillisForOldTransaction;

    public Configuration(
        int replicationDbPartitionSize,
        int threadSize,
        int waitMillisPerPartition,
        int fetchSize,
        long thresholdMillisForOldTransaction) {
      super(replicationDbPartitionSize, threadSize, waitMillisPerPartition);
      this.fetchSize = fetchSize;
      this.thresholdMillisForOldTransaction = thresholdMillisForOldTransaction;
    }
  }

  public OldTransactionHandlerWorker(
      Configuration conf,
      CoordinatorStateRepository coordinatorStateRepository,
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationRecordRepository replicationRecordRepository,
      RecordHandler recordHandler,
      ExecutorService recordHandlerExecutorService,
      MetricsLogger metricsLogger) {
    super(conf, "tx", metricsLogger);
    this.conf = conf;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.recordHandler = recordHandler;
    this.recordHandlerExecutorService = recordHandlerExecutorService;
    this.metricsLogger = metricsLogger;
  }

  private void handleWrittenTuple(
      String transactionId, WrittenTuple writtenTuple, Instant committedAt)
      throws ExecutionException {
    Key key =
        replicationRecordRepository.createKey(
            writtenTuple.namespace,
            writtenTuple.table,
            writtenTuple.partitionKey,
            writtenTuple.clusteringKey);

    Value newValue;
    if (writtenTuple instanceof InsertedTuple) {
      InsertedTuple tuple = (InsertedTuple) writtenTuple;
      newValue =
          new Value(
              null,
              transactionId,
              writtenTuple.txVersion,
              writtenTuple.txPreparedAtInMillis,
              committedAt.toEpochMilli(),
              "insert",
              tuple.columns);
    } else if (writtenTuple instanceof UpdatedTuple) {
      UpdatedTuple tuple = (UpdatedTuple) writtenTuple;
      newValue =
          new Value(
              tuple.prevTxId,
              transactionId,
              writtenTuple.txVersion,
              writtenTuple.txPreparedAtInMillis,
              committedAt.toEpochMilli(),
              "update",
              tuple.columns);
    } else if (writtenTuple instanceof DeletedTuple) {
      DeletedTuple tuple = (DeletedTuple) writtenTuple;
      newValue =
          new Value(
              tuple.prevTxId,
              transactionId,
              writtenTuple.txVersion,
              writtenTuple.txPreparedAtInMillis,
              committedAt.toEpochMilli(),
              "delete",
              null);
    } else {
      throw new AssertionError();
    }

    /*
    try {
      metricsLogger.execAppendValueToRecord(
          () -> {
            replicationRecordRepository.upsertWithNewValue(key, newValue);
            return null;
          });
    } catch (Exception e) {
      String message =
          String.format(
              "Failed to append the value. key:%s, txId:%s, newValue:%s",
              key, transactionId, newValue);
      throw new RuntimeException(message, e);
    }
     */

    retryOnConflictException(
        "copy a written tuple to the record",
        key,
        () ->
            metricsLogger.execAppendValueToRecord(
                () -> {
                  //                  Optional<Record> recordOpt =
                  // replicationRecordRepository.get(key);
                  // Add the new values to the record.
                  replicationRecordRepository.upsertWithNewValue(key, newValue);
                  return null;
                }));

    retryOnConflictException(
        "handle the written tuple",
        key,
        () -> {
          // Handle the new value.
          while (true) {
            ResultOfKeyHandling resultOfKeyHandling = recordHandler.handleKey(key, true);
            if (resultOfKeyHandling.nextConnectedValueExists) {
              // Need to handle the connected value immediately.
            } else {
              break;
            }
          }
        });
  }

  private boolean isConflictException(Throwable exception) {
    Throwable e = exception;
    while (e != null) {
      if (e instanceof NoMutationException) {
        return true;
      }

      e = e.getCause();
    }
    return false;
  }

  private void retryOnConflictException(
      String taskDesc, Key key, FailableRunnable<ExecutionException> task)
      throws ExecutionException {
    while (true) {
      try {
        task.run();
        return;
      } catch (Exception e) {
        if (isConflictException(e)) {
          logger.warn("Failed to {} due to conflict. Retrying... Key:{}", taskDesc, key, e);
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Copy tuples to Records table
   *
   * @param transaction A transaction
   * @return true if the transaction has finished, false otherwise. private boolean
   *     copyTuplesToRecords(Transaction transaction) throws ExecutionException {
   *     metricsLogger.incrementScannedTransactions(); Optional<CoordinatorState> coordinatorState =
   *     coordinatorStateRepository.get(transaction.transactionId); if
   *     (!coordinatorState.isPresent()) { metricsLogger.incrementUncommittedTransactions(); Instant
   *     now = Instant.now(); if
   *     (transaction.updatedAt.isBefore(now.minusMillis(conf.thresholdMillisForOldTransaction))) {
   *     logger.info( "Updating an old transaction to be handled later. txId:{}",
   *     transaction.transactionId); replicationTransactionRepository.updateUpdatedAt(transaction);
   *     } return false; } if (coordinatorState.get().txState != TransactionState.COMMITTED) { //
   *     TODO: Add AbortedTransactions metricsLogger.incrementUncommittedTransactions(); return
   *     true; }
   *     <p>// Copy written tuples to `records` table for (WrittenTuple writtenTuple :
   *     transaction.writtenTuples) { handleWrittenTuple( transaction.transactionId, writtenTuple,
   *     coordinatorState.get().txCommittedAt); }
   *     metricsLogger.incrementHandledCommittedTransactions(); return true; }
   */

  // TODO: This should be moved to TransactionHandler and executed by queue consumers.
  Optional<Transaction> handleTransaction(Transaction transaction) throws Exception {
    metricsLogger.incrementScannedTransactions();
    Optional<CoordinatorState> coordinatorState =
        coordinatorStateRepository.get(transaction.transactionId);
    if (!coordinatorState.isPresent()) {
      metricsLogger.incrementUncommittedTransactions();
      // TODO: Maybe always updating `updated_at` works better.
      if (transaction.updatedAt.isBefore(
          Instant.now().minusMillis(conf.thresholdMillisForOldTransaction))) {
        logger.info(
            "Updating an ongoing transaction to be handled later. txId:{}",
            transaction.transactionId);
        // TODO
        //        Transaction updatedTransaction =
        //            replicationTransactionRepository.updateUpdatedAt(transaction);
        //        return Optional.of(updatedTransaction);
        replicationTransactionRepository.updateUpdatedAt(transaction);
        return Optional.of(transaction);
      } else {
        return Optional.of(transaction);
      }
    }
    if (coordinatorState.get().txState != TransactionState.COMMITTED) {
      // TODO
      //      metricsLogger.incrementAbortedTransactions();
      replicationTransactionRepository.delete(transaction);
      return Optional.empty();
    }

    // Handle the written tuples.
    List<Future<?>> futures = new ArrayList<>(transaction.writtenTuples.size());
    for (WrittenTuple writtenTuple : transaction.writtenTuples) {
      logger.debug(
          "[handleTransaction]\n  transaction:{}\n  writtenTuple:{}\n", transaction, writtenTuple);

      futures.add(
          recordHandlerExecutorService.submit(
              () -> {
                try {
                  handleWrittenTuple(
                      transaction.transactionId,
                      writtenTuple,
                      coordinatorState.get().txCommittedAt);
                } catch (Exception e) {
                  throw new RuntimeException(
                      String.format(
                          "Failed to handle written tuples unexpectedly. Namespace:%s, Table:%s, Partition key:%s, Clustering key:%s",
                          writtenTuple.namespace,
                          writtenTuple.table,
                          writtenTuple.partitionKey,
                          writtenTuple.clusteringKey),
                      e);
                }
              }));
    }
    for (Future<?> future : futures) {
      // If a timeout occurs unexpectedly, all the written tuples will be retries.
      Uninterruptibles.getUninterruptibly(future, 60, TimeUnit.SECONDS);
    }

    metricsLogger.incrementHandledCommittedTransactions();
    replicationTransactionRepository.delete(transaction);
    return Optional.empty();
  }

  @Override
  protected boolean handle(int partitionId) throws Exception {
    List<Transaction> scannedTxns =
        metricsLogger.execFetchTransactions(
            () -> replicationTransactionRepository.scan(partitionId, conf.fetchSize));
    int finishedTransactions = 0;
    for (Transaction transaction : scannedTxns) {
      if (!handleTransaction(transaction).isPresent()) {
        //        replicationTransactionRepository.delete(transaction);
        finishedTransactions++;
      }
    }

    return finishedTransactions >= conf.fetchSize;
  }
}
