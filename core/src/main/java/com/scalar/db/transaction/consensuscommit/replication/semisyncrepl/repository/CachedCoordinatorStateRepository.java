package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import com.scalar.db.transaction.consensuscommit.Coordinator.State;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.MetricsLogger;
import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import java.time.Duration;
import java.util.Optional;

public class CachedCoordinatorStateRepository extends CoordinatorStateRepository {
  // ParentTxnId -> Coordinator.State
  private final Cache<String, State> cache;
  private final CoordinatorGroupCommitKeyManipulator groupCommitKeyManipulator =
      new CoordinatorGroupCommitKeyManipulator();
  private final MetricsLogger metricsLogger;

  private Cache<String, State> createCache() {
    return CacheBuilder.newBuilder()
        // TODO: Make these configurable.
        .expireAfterWrite(Duration.ofMinutes(1))
        .maximumSize(100000)
        .build();
  }

  public CachedCoordinatorStateRepository(
      DistributedStorage coordinatorDbStorage,
      ConsensusCommitConfig consensusCommitConfig,
      MetricsLogger metricsLogger) {
    super(coordinatorDbStorage, consensusCommitConfig);
    cache = createCache();
    this.metricsLogger = metricsLogger;
  }

  @Override
  public Optional<Coordinator.State> get(String transactionId) throws ExecutionException {
    // If the transaction wasn't group committed, just read the coordinator table and return the
    // state without using the cache.
    if (!groupCommitKeyManipulator.isFullKey(transactionId)) {
      return super.get(transactionId);
    }

    Keys<String, String, String> groupCommitTxKeys =
        groupCommitKeyManipulator.keysFromFullKey(transactionId);

    // If the parent Tx ID is contained in the cache and the cached state contains the child Tx ID,
    // return the cached state.
    State cachedState = cache.getIfPresent(groupCommitTxKeys.parentKey);
    // TODO: Move this logic to Coordinator.
    if (cachedState != null && cachedState.getChildIds().contains(groupCommitTxKeys.childKey)) {
      metricsLogger.incrementCoordStateCacheHit();
      return Optional.of(cachedState);
    }
    metricsLogger.incrementCoordStateCacheMiss();

    // Read the coordinator table.
    Optional<State> state = super.get(transactionId);
    if (!state.isPresent()) {
      return Optional.empty();
    }

    // If the transaction ID contains only the parent Tx ID, put the state into the cache.
    if (!groupCommitKeyManipulator.isFullKey(state.get().getId())) {
      cache.put(groupCommitTxKeys.parentKey, state.get());
    }

    return state;
  }
}
