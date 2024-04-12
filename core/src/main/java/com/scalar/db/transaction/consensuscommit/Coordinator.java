package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionState;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.transaction.consensuscommit.CoordinatorGroupCommitter.CoordinatorGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.KeyManipulator.Keys;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class Coordinator {
  public static final String NAMESPACE = "coordinator";
  public static final String TABLE = "state";
  public static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(Attribute.ID, DataType.TEXT)
          .addColumn(Attribute.CHILD_IDS, DataType.TEXT)
          .addColumn(Attribute.STATE, DataType.INT)
          .addColumn(Attribute.CREATED_AT, DataType.BIGINT)
          .addPartitionKey(Attribute.ID)
          .addColumn("tx_sub_id", DataType.TEXT)
          .addClusteringKey("tx_sub_id") // For the delayed transactions of GroupCommit
          .build();
  private static final String DEFAULT_SUB_ID_VALUE = "<<DEFAULT>>";
  private static final Key DEFAULT_SUB_ID_KEY = Key.ofText("tx_sub_id", DEFAULT_SUB_ID_VALUE);

  private static final int MAX_RETRY_COUNT = 5;
  private static final long SLEEP_BASE_MILLIS = 50;
  private static final Logger logger = LoggerFactory.getLogger(Coordinator.class);
  private final DistributedStorage storage;
  private final String coordinatorNamespace;
  private final CoordinatorGroupCommitKeyManipulator keyManipulator;

  /**
   * @param storage a storage
   * @deprecated As of release 3.3.0. Will be removed in release 5.0.0
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  @Deprecated
  public Coordinator(DistributedStorage storage) {
    this.storage = storage;
    coordinatorNamespace = NAMESPACE;
    keyManipulator = new CoordinatorGroupCommitKeyManipulator();
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public Coordinator(DistributedStorage storage, ConsensusCommitConfig config) {
    this.storage = storage;
    coordinatorNamespace = config.getCoordinatorNamespace().orElse(NAMESPACE);
    keyManipulator = new CoordinatorGroupCommitKeyManipulator();
  }

  public Optional<Coordinator.State> getState(String id) throws CoordinatorException {
    if (keyManipulator.isFullKey(id)) {
      // Scan with the parent ID for group committed record.
      return getStateForGroupCommit(id);
    }

    Get get = createGetWith(id);
    return get(get);
  }

  @VisibleForTesting
  Optional<Coordinator.State> getStateForGroupCommit(String id) throws CoordinatorException {
    if (!keyManipulator.isFullKey(id)) {
      throw new IllegalArgumentException("This id format isn't for group commit. Id:" + id);
    }
    Keys<String, String, String> idForGroupCommit = keyManipulator.keysFromFullKey(id);

    String parentId = idForGroupCommit.parentKey;
    String childId = idForGroupCommit.childKey;
    Scan scan =
        Scan.newBuilder()
            .namespace(coordinatorNamespace)
            .table(TABLE)
            .partitionKey(Key.ofText(Attribute.ID, parentId))
            .consistency(Consistency.LINEARIZABLE)
            .build();

    State defaultGroupCommitState = null;
    for (State s : scan(scan)) {
      if (childId.equals(s.getSubId())) {
        return Optional.of(s);
      } else if (DEFAULT_SUB_ID_VALUE.equals(s.getSubId())) {
        defaultGroupCommitState = s;
      }
    }

    if (defaultGroupCommitState != null && defaultGroupCommitState.childIds.contains(childId)) {
      return Optional.of(defaultGroupCommitState);
    }

    return Optional.empty();
  }

  public void putState(Coordinator.State state) throws CoordinatorException {
    Put put = createPutWith(state);
    put(put);
  }

  void putStateForGroupCommit(
      String id, List<String> fullIds, TransactionState transactionState, long createdAt)
      throws CoordinatorException {
    State state;
    if (keyManipulator.isFullKey(id)) {
      // Group commit for single transaction
      // In this case, this is same as normal commit and child_ids isn't needed.
      assert (fullIds.isEmpty());
      state = new State(id, transactionState, createdAt);
    } else {
      // Group commit with child_ids.
      List<String> childIds = new ArrayList<>(fullIds.size());
      for (String fullId : fullIds) {
        if (!keyManipulator.isFullKey(fullId)) {
          throw new IllegalStateException(
              String.format(
                  "The full transaction ID for group commit is invalid format. ID:%s, FullIds:%s, State:%s, CreatedAt:%s",
                  id, fullId, transactionState, createdAt));
        }
        Keys<String, String, String> keys = keyManipulator.keysFromFullKey(fullId);
        childIds.add(keys.childKey);
      }
      state = new State(id, childIds, transactionState, createdAt);
    }
    put(createPutWith(state));
  }

  // TODO: Make this more generic to use for Get?
  private List<State> scan(Scan scan) throws CoordinatorException {
    int counter = 0;
    Exception lastException = null;
    while (true) {
      if (counter >= MAX_RETRY_COUNT) {
        throw new CoordinatorException("Can't scan coordinator states", lastException);
      }
      try {
        List<State> states = new ArrayList<>();
        try (Scanner scanner = storage.scan(scan)) {
          for (Result result : scanner.all()) {
            State state = new State(result);
            states.add(state);
          }
        }
        return states;
      } catch (ExecutionException | IOException e) {
        lastException = e;
        logger.warn("Can't get coordinator state", e);
      }
      exponentialBackoff(counter++);
    }
  }

  private Get createGetWith(String id) {
    return new Get(new Key(Attribute.toIdValue(id)), DEFAULT_SUB_ID_KEY)
        .withConsistency(Consistency.LINEARIZABLE)
        .forNamespace(coordinatorNamespace)
        .forTable(TABLE);
  }

  private Optional<Coordinator.State> get(Get get) throws CoordinatorException {
    int counter = 0;
    while (true) {
      if (counter >= MAX_RETRY_COUNT) {
        throw new CoordinatorException("Can't get coordinator state");
      }
      try {
        Optional<Result> result = storage.get(get);
        if (result.isPresent()) {
          return Optional.of(new State(result.get()));
        } else {
          return Optional.empty();
        }
      } catch (ExecutionException e) {
        logger.warn("Can't get coordinator state", e);
      }
      exponentialBackoff(counter++);
    }
  }

  @VisibleForTesting
  Put createPutWith(Coordinator.State state) {
    if (keyManipulator.isFullKey(state.getId())) {
      Keys<String, String, String> keys = keyManipulator.keysFromFullKey(state.getId());
      return new Put(
              new Key(Attribute.toIdValue(keys.parentKey)), Key.ofText("tx_sub_id", keys.childKey))
          .withValue(Attribute.toStateValue(state.getState()))
          .withValue(Attribute.toCreatedAtValue(state.getCreatedAt()))
          .withConsistency(Consistency.LINEARIZABLE)
          .withCondition(new PutIfNotExists())
          .forNamespace(coordinatorNamespace)
          .forTable(TABLE);
    } else {
      return new Put(new Key(Attribute.toIdValue(state.getId())), DEFAULT_SUB_ID_KEY)
          .withValue(Attribute.toChildIdsValue(Joiner.on(',').join(state.getChildIds())))
          .withValue(Attribute.toStateValue(state.getState()))
          .withValue(Attribute.toCreatedAtValue(state.getCreatedAt()))
          .withConsistency(Consistency.LINEARIZABLE)
          .withCondition(new PutIfNotExists())
          .forNamespace(coordinatorNamespace)
          .forTable(TABLE);
    }
  }

  private void put(Put put) throws CoordinatorException {
    int counter = 0;
    while (true) {
      if (counter >= MAX_RETRY_COUNT) {
        throw new CoordinatorException("Couldn't put coordinator state");
      }
      try {
        storage.put(put);
        break;
      } catch (NoMutationException e) {
        throw new CoordinatorConflictException("Mutation seems applied already", e);
      } catch (ExecutionException e) {
        logger.warn("Putting state in coordinator failed", e);
      }
      exponentialBackoff(counter++);
    }
  }

  private void exponentialBackoff(int counter) {
    Uninterruptibles.sleepUninterruptibly(
        (long) Math.pow(2, counter) * SLEEP_BASE_MILLIS, TimeUnit.MILLISECONDS);
  }

  @ThreadSafe
  public static class State {
    private static final List<String> EMPTY_CHILD_IDS = Collections.emptyList();
    private static final String CHILD_IDS_DELIMITER = ",";
    private final String id;
    @Nullable private final String subId;
    private final TransactionState state;
    private final long createdAt;
    private final List<String> childIds;

    public State(Result result) throws CoordinatorException {
      checkNotMissingRequired(result);
      id = result.getValue(Attribute.ID).get().getAsString().get();
      subId = result.getValue("tx_sub_id").get().getAsString().orElse(null);
      state = TransactionState.getInstance(result.getValue(Attribute.STATE).get().getAsInt());
      createdAt = result.getValue(Attribute.CREATED_AT).get().getAsLong();
      Optional<String> childIdsOpt = result.getValue(Attribute.CHILD_IDS).get().getAsString();
      childIds =
          childIdsOpt
              .map(s -> Splitter.on(CHILD_IDS_DELIMITER).omitEmptyStrings().splitToList(s))
              .orElse(EMPTY_CHILD_IDS);
    }

    public State(String id, TransactionState state) {
      this(id, state, System.currentTimeMillis());
    }

    // For the SpotBugs warning CT_CONSTRUCTOR_THROW
    @Override
    protected final void finalize() {}

    @VisibleForTesting
    State(String id, List<String> childIds, TransactionState state, long createdAt) {
      this.id = checkNotNull(id);
      this.subId = DEFAULT_SUB_ID_VALUE;
      for (String childId : childIds) {
        if (childId.contains(CHILD_IDS_DELIMITER)) {
          throw new IllegalArgumentException(
              String.format(
                  "This child transaction ID itself contains the delimiter. ChildTransactionID: %s, Delimiter: %s",
                  childId, CHILD_IDS_DELIMITER));
        }
      }
      this.childIds = childIds;
      this.state = checkNotNull(state);
      this.createdAt = createdAt;
    }

    @VisibleForTesting
    State(String id, TransactionState state, long createdAt) {
      this(id, EMPTY_CHILD_IDS, state, createdAt);
    }

    @Nonnull
    public String getId() {
      return id;
    }

    @Nullable
    public String getSubId() {
      return subId;
    }

    @Nonnull
    public TransactionState getState() {
      return state;
    }

    public long getCreatedAt() {
      return createdAt;
    }

    public List<String> getChildIds() {
      return childIds;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof State)) return false;
      State other = (State) o;
      // NOTICE: createdAt is not taken into account
      return Objects.equals(id, other.id)
          && Objects.equals(subId, other.subId)
          && state == other.state
          && Objects.equals(childIds, other.childIds);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, subId, state, childIds);
    }

    private void checkNotMissingRequired(Result result) throws CoordinatorException {
      if (!result.getValue(Attribute.ID).isPresent()
          || !result.getValue(Attribute.ID).get().getAsString().isPresent()) {
        throw new CoordinatorException("id is missing in the coordinator state");
      }
      // TODO: tx_sub_id
      if (!result.getValue(Attribute.STATE).isPresent()
          || result.getValue(Attribute.STATE).get().getAsInt() == 0) {
        throw new CoordinatorException("state is missing in the coordinator state");
      }
      if (!result.getValue(Attribute.CREATED_AT).isPresent()
          || result.getValue(Attribute.CREATED_AT).get().getAsLong() == 0) {
        throw new CoordinatorException("created_at is missing in the coordinator state");
      }
    }
  }
}
