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
import org.apache.commons.lang3.function.FailableRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TransactionHandler {
  private static final Logger logger = LoggerFactory.getLogger(TransactionHandler.class);
  private final long oldTransactionThresholdMillis;
  private final ReplicationTransactionRepository replicationTransactionRepository;
  private final ReplicationRecordRepository replicationRecordRepository;
  private final CoordinatorStateRepository coordinatorStateRepository;
  private final RecordHandler recordHandler;
  private final MetricsLogger metricsLogger;

  public TransactionHandler(
      long oldTransactionThresholdMillis,
      ReplicationTransactionRepository replicationTransactionRepository,
      ReplicationRecordRepository replicationRecordRepository,
      CoordinatorStateRepository coordinatorStateRepository,
      RecordHandler recordHandler,
      MetricsLogger metricsLogger) {
    this.oldTransactionThresholdMillis = oldTransactionThresholdMillis;
    this.replicationTransactionRepository = replicationTransactionRepository;
    this.replicationRecordRepository = replicationRecordRepository;
    this.coordinatorStateRepository = coordinatorStateRepository;
    this.recordHandler = recordHandler;
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

    // These operations are retried separately since appending value doesn't need to be retried
    // once it finished successfully.
    retryOnConflictException(
        "copy a written tuple to the record",
        key,
        () ->
            metricsLogger.execAppendValueToRecord(
                () -> {
                  // Add the new values to the record.
                  replicationRecordRepository.upsertWithNewValue(key, newValue);
                  return null;
                }));

    retryOnConflictException(
        "handle the written tuple",
        key,
        () -> {
          while (true) {
            ResultOfKeyHandling resultOfKeyHandling = recordHandler.handleKey(key, true);
            if (resultOfKeyHandling.nextConnectedValueExists) {
              // There is a connected value in the record. Need to retry immediately.
            } else {
              break;
            }
          }
        });
  }

  // TODO: Avoid the duplication.
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
          Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * Handle a transaction
   *
   * @param transaction A transaction
   * @return true if handling the transaction is done, false otherwise.
   */
  boolean handleTransaction(ExecutorService executorService, Transaction transaction)
      throws Exception {
    Optional<CoordinatorState> coordinatorState =
        coordinatorStateRepository.get(transaction.transactionId);
    if (!coordinatorState.isPresent()) {
      metricsLogger.incrementUncommittedTransactions();
      // This logic might be needed in TransactionScanner.
      /*
      if (transaction.updatedAt.isBefore(
          Instant.now().minusMillis(oldTransactionThresholdMillis))) {
        logger.info(
            "Updating an ongoing transaction to be handled later. txId:{}",
            transaction.transactionId);
        Transaction updatedTransaction =
            replicationTransactionRepository.updateUpdatedAt(transaction);
        return Optional.of(updatedTransaction);
      } else {
        return Optional.of(transaction);
      }
       */
      return false;
    }
    if (coordinatorState.get().txState != TransactionState.COMMITTED) {
      metricsLogger.incrementAbortedTransactions();
      replicationTransactionRepository.delete(transaction);
      return true;
    }

    // Handle the written tuples.
    List<Future<?>> futures = new ArrayList<>(transaction.writtenTuples.size());
    for (WrittenTuple writtenTuple : transaction.writtenTuples) {
      logger.debug(
          "[handleTransaction]\n  transaction:{}\n  writtenTuple:{}\n", transaction, writtenTuple);

      // TODO: Handle the last item with its own thread.
      futures.add(
          executorService.submit(
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

    metricsLogger.incrCommittedTxns();
    replicationTransactionRepository.delete(transaction);

    return true;
  }
}
