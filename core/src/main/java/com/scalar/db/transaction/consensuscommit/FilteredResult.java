package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import com.scalar.db.util.AbstractResult;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@code Result} to filter out unprojected columns and transactional columns.
 */
@Immutable
public class FilteredResult extends AbstractResult {
  private static final Logger LOGGER = LoggerFactory.getLogger(FilteredResult.class);

  private final Result original;
  private final Set<String> containedColumnNames;

  private final Supplier<Map<String, Value<?>>> valuesWithDefaultValues;

  public FilteredResult(Result original, List<String> projections, TableMetadata metadata) {
    this.original = Objects.requireNonNull(original);

    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    original.getContainedColumnNames().stream()
        .filter(c -> projections.isEmpty() || projections.contains(c))
        .filter(c -> !ConsensusCommitUtils.isTransactionalMetaColumn(c, metadata))
        .forEach(builder::add);
    containedColumnNames = builder.build();

    // lazy loading
    valuesWithDefaultValues =
        Suppliers.memoize(
            () -> {
              ImmutableMap.Builder<String, Value<?>> valuesBuilder = ImmutableMap.builder();
              original.getValues().entrySet().stream()
                  .filter(e -> containedColumnNames.contains(e.getKey()))
                  .forEach(e -> valuesBuilder.put(e.getKey(), e.getValue()));
              return valuesBuilder.build();
            });
  }

  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(original.getPartitionKey());
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(original.getClusteringKey());
  }

  private Optional<Key> getKey(Optional<Key> key) {
    if (!key.isPresent()) {
      return Optional.empty();
    }
    for (Value<?> value : key.get()) {
      if (!containedColumnNames.contains(value.getName())) {
        LOGGER.warn("full key doesn't seem to be projected into the result");
        return Optional.empty();
      }
    }
    return key;
  }

  @Deprecated
  @Override
  public Optional<Value<?>> getValue(String columnName) {
    return Optional.ofNullable(valuesWithDefaultValues.get().get(columnName));
  }

  @Deprecated
  @Override
  public Map<String, Value<?>> getValues() {
    return valuesWithDefaultValues.get();
  }

  @Override
  public boolean isNull(String columnName) {
    checkIfExists(columnName);
    return original.isNull(columnName);
  }

  @Override
  public boolean getBoolean(String columnName) {
    checkIfExists(columnName);
    return original.getBoolean(columnName);
  }

  @Override
  public int getInt(String columnName) {
    checkIfExists(columnName);
    return original.getInt(columnName);
  }

  @Override
  public long getBigInt(String columnName) {
    checkIfExists(columnName);
    return original.getBigInt(columnName);
  }

  @Override
  public float getFloat(String columnName) {
    checkIfExists(columnName);
    return original.getFloat(columnName);
  }

  @Override
  public double getDouble(String columnName) {
    checkIfExists(columnName);
    return original.getDouble(columnName);
  }

  @Nullable
  @Override
  public String getText(String columnName) {
    checkIfExists(columnName);
    return original.getText(columnName);
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnName) {
    checkIfExists(columnName);
    return original.getBlobAsByteBuffer(columnName);
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnName) {
    checkIfExists(columnName);
    return original.getBlobAsBytes(columnName);
  }

  @Nullable
  @Override
  public Object getAsObject(String columnName) {
    checkIfExists(columnName);
    return original.getAsObject(columnName);
  }

  @Override
  public boolean contains(String columnName) {
    return containedColumnNames.contains(columnName);
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return containedColumnNames;
  }

  @VisibleForTesting
  Result getOriginalResult() {
    return original;
  }
}
