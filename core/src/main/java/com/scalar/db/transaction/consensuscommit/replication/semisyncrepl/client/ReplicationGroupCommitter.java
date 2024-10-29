package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationGroupCommitter<V> {
  private static final Logger logger = LoggerFactory.getLogger(ReplicationGroupCommitter.class);
  private final BlockingQueue<Itemable<V>> queue = new LinkedBlockingQueue<>();
  private final AtomicReference<BufferedValueItems<V>> currentFetchedItems =
      new AtomicReference<>();
  private final long retentionTimeInMillis;
  private final int numberOfRetentionValues;
  private final long expirationCheckIntervalInMillis;
  private final ExecutorService dequeueExecutorService;
  private final ExecutorService emitExecutorService;
  private final ScheduledExecutorService expirationCheckExecutorService;
  private final Consumer<List<V>> emitter;
  private final boolean debugMode;

  private interface Itemable<V> {}

  static class ValueItem<V> implements Itemable<V> {
    public final V value;
    public final CompletableFuture<Void> future;

    public ValueItem(V value, CompletableFuture<Void> future) {
      this.value = value;
      this.future = future;
    }
  }

  static class WakeupItem<V> implements Itemable<V> {}

  // TODO: Rename to BufferedValues?
  static class BufferedValueItems<V> {
    public final List<ValueItem<V>> values = new ArrayList<>();
    public final Long createdAtInMilli;

    public BufferedValueItems(Long createdAtInMilli) {
      this.createdAtInMilli = createdAtInMilli;
    }
  }

  public ReplicationGroupCommitter(
      String label,
      long retentionTimeInMillis,
      int numberOfRetentionValues,
      long expirationCheckIntervalInMillis,
      int numberOfThreads,
      Consumer<List<V>> emitter,
      boolean debugMode) {
    this.retentionTimeInMillis = retentionTimeInMillis;
    this.numberOfRetentionValues = numberOfRetentionValues;
    this.expirationCheckIntervalInMillis = expirationCheckIntervalInMillis;
    this.emitter = emitter;
    this.debugMode = debugMode;

    this.dequeueExecutorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-dequeue-%d")
                .build());

    this.emitExecutorService =
        Executors.newFixedThreadPool(
            numberOfThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-emit-%d")
                .build());

    this.expirationCheckExecutorService =
        Executors.newScheduledThreadPool(
            1,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(label + "-group-commit-expire-%d")
                .build());

    startDequeueExecutorService();
    startExpirationCheckExecutorService();
  }

  private void emitFetchedItemsIfNeeded(BufferedValueItems<V> bufferedValueItems) {
    if (bufferedValueItems.values.size() >= numberOfRetentionValues
        || bufferedValueItems.createdAtInMilli + retentionTimeInMillis
            < System.currentTimeMillis()) {
      currentFetchedItems.set(null);
      emitExecutorService.submit(
          () -> {
            try {
              if (debugMode) {
                logger.info(
                    "Emitting (thread_id:{}, num_of_values:{})",
                    Thread.currentThread().getId(),
                    bufferedValueItems.values.size());
              }
              emitter.accept(
                  bufferedValueItems.values.stream()
                      .map(vf -> vf.value)
                      .collect(Collectors.toList()));
              if (debugMode) {
                logger.info(
                    "Emitted (thread_id:{}, num_of_values:{})",
                    Thread.currentThread().getId(),
                    bufferedValueItems.values.size());
              }
              bufferedValueItems.values.forEach(vf -> vf.future.complete(null));
              if (debugMode) {
                logger.info(
                    "Notified (thread_id:{}, num_of_values:{})",
                    Thread.currentThread().getId(),
                    bufferedValueItems.values.size());
              }
            } catch (Throwable e) {
              bufferedValueItems.values.forEach(vf -> vf.future.completeExceptionally(e));
            }
          });
    }
  }

  private void handleItem(Itemable<V> itemable) {
    if (itemable instanceof ValueItem) {
      ValueItem<V> item = (ValueItem<V>) itemable;
      BufferedValueItems<V> bufferedValueItems =
          currentFetchedItems.updateAndGet(
              current -> {
                if (current == null) {
                  return new BufferedValueItems<>(System.currentTimeMillis());
                }
                return current;
              });
      // All values in a queue need to use the same unique key for partition key.
      bufferedValueItems.values.add(item);
      emitFetchedItemsIfNeeded(bufferedValueItems);
    } else if (itemable instanceof WakeupItem) {
      BufferedValueItems<V> bufferedValueItems = currentFetchedItems.get();
      if (bufferedValueItems != null) {
        // FIXME: Wrap an exception
        emitFetchedItemsIfNeeded(bufferedValueItems);
      }
    } else {
      logger.error("Fetched an unexpected item. Skipping " + itemable);
    }
  }

  private void startDequeueExecutorService() {
    dequeueExecutorService.submit(
        () -> {
          while (true) {
            try {
              handleItem(queue.take());
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              logger.warn("Interrupted", e);
              break;
            }
          }
        });
  }

  private void startExpirationCheckExecutorService() {
    expirationCheckExecutorService.scheduleAtFixedRate(
        this::emitIfExpired,
        expirationCheckIntervalInMillis,
        expirationCheckIntervalInMillis,
        TimeUnit.MILLISECONDS);
  }

  public void addValue(V value) throws ReplicationGroupCommitException {
    CompletableFuture<Void> future = new CompletableFuture<>();
    queue.add(new ValueItem<>(value, future));
    try {
      long start = System.currentTimeMillis();
      if (debugMode) {
        logger.info("Wait start(thread_id:{})", Thread.currentThread().getId());
      }
      future.get();
      if (debugMode) {
        logger.info(
            "Wait end(thread_id:{}): {} ms",
            Thread.currentThread().getId(),
            System.currentTimeMillis() - start);
      }
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ReplicationGroupCommitCascadeException) {
        throw (ReplicationGroupCommitCascadeException) e.getCause();
      } else if (e.getCause() instanceof ReplicationGroupCommitException) {
        throw (ReplicationGroupCommitException) e.getCause();
      }
      throw new ReplicationGroupCommitException("Failed to group-commit", e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted", e);
    }
  }

  void emitIfExpired() {
    BufferedValueItems<V> bufferedValueItems = currentFetchedItems.get();
    if (bufferedValueItems != null
        && !bufferedValueItems.values.isEmpty()
        && bufferedValueItems.createdAtInMilli + retentionTimeInMillis
            < System.currentTimeMillis()) {
      queue.add(new WakeupItem<>());
    }
  }
}
