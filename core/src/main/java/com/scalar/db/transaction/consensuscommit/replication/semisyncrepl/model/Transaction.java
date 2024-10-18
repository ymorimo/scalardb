package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Transaction implements Comparable<Transaction> {
  public final int partitionId;
  public final Instant createdAt;
  public final String transactionId;
  public final Collection<WrittenTuple> writtenTuples;
  // FIXME
  @JsonIgnore public final String type = null;

  public Transaction(
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("createdAt") Instant createdAt,
      @JsonProperty("transactionId") String transactionId,
      @JsonProperty("writtenTuples") Collection<WrittenTuple> writtenTuples) {
    this.partitionId = partitionId;
    this.createdAt = createdAt;
    this.transactionId = transactionId;
    this.writtenTuples = writtenTuples;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Transaction)) return false;
    Transaction that = (Transaction) o;
    return partitionId == that.partitionId
        && Objects.equals(createdAt, that.createdAt)
        && Objects.equals(transactionId, that.transactionId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionId, createdAt, transactionId);
  }

  @Override
  public int compareTo(Transaction o) {
    if (partitionId < o.partitionId) {
      return -1;
    } else if (partitionId > o.partitionId) {
      return 1;
    }

    if (createdAt.toEpochMilli() < o.createdAt.toEpochMilli()) {
      return -1;
    } else if (createdAt.toEpochMilli() > o.createdAt.toEpochMilli()) {
      return 1;
    }

    return transactionId.compareTo(o.transactionId);
  }
}
