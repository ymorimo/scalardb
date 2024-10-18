package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.google.common.base.MoreObjects;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
// TODO: Rename this to BulkWriteSet or something
public class BulkTransaction {
  public final int partitionId;
  public final Instant createdAt;
  public final String uniqueId;
  public final Collection<Transaction> transactions;

  public BulkTransaction(
      int partitionId, Instant createdAt, String uniqueId, Collection<Transaction> transactions) {
    this.partitionId = partitionId;
    this.createdAt = createdAt;
    this.uniqueId = uniqueId;
    this.transactions = transactions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BulkTransaction)) return false;
    BulkTransaction that = (BulkTransaction) o;
    return partitionId == that.partitionId
        && Objects.equals(createdAt, that.createdAt)
        && Objects.equals(uniqueId, that.uniqueId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionId, createdAt, uniqueId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partitionId", partitionId)
        .add("createdAt", createdAt)
        .add("uniqueId", uniqueId)
        .add("transactions", transactions)
        .toString();
  }
}
