package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.Utils;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

// TODO: Rename this to WriteOperation
public class Record {
  public final RecordKey key;
  public final long version;
  @Nullable public final String currentTxId;
  public final Set<Value> values;
  public final boolean deleted;
  // TODO: `deleted` is added, so we can remove this field?
  //       Probably "no" considering retry by TransactionScanWorker.
  public final Set<String> insertTxIds;

  /** A class that represents a write-set in `records` table. See also {@link WrittenTuple}. */
  public static class Value {
    public final String prevTxId;
    public final String txId;
    public final int txVersion;
    public final long txPreparedAtInMillis;
    public final long txCommittedAtInMillis;

    // TODO: This can be an enum.
    public final String type;
    public final Collection<Column<?>> columns;

    public Value(
        @JsonProperty("prevTxId") String prevTxId,
        @JsonProperty("txId") String txId,
        @JsonProperty("txVersion") int txVersion,
        @JsonProperty("txPreparedAtInMillis") long txPreparedAtInMillis,
        @JsonProperty("txCommittedAtInMillis") long txCommittedAtInMillis,
        @JsonProperty("type") String type,
        @JsonProperty("columns") Collection<Column<?>> columns) {
      this.prevTxId = prevTxId;
      this.txId = txId;
      this.txVersion = txVersion;
      this.txPreparedAtInMillis = txPreparedAtInMillis;
      this.txCommittedAtInMillis = txCommittedAtInMillis;
      this.type = type;
      this.columns = columns;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Value)) {
        return false;
      }
      Value value = (Value) o;
      return Objects.equals(txId, value.txId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(txId);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("prevTxId", prevTxId)
          .add("txId", txId)
          .add("txVersion", txVersion)
          .add("txPreparedAtInMillis", txPreparedAtInMillis)
          .add("txCommittedAtInMillis", txCommittedAtInMillis)
          .add("type", type)
          .add("columns", columns)
          .toString();
    }

    public String toStringOnlyWithMetadata() {
      return MoreObjects.toStringHelper(this)
          .add("prevTxId", prevTxId)
          .add("txId", txId)
          .add("txVersion", txVersion)
          .add("txPreparedAtInMillis", txPreparedAtInMillis)
          .add("txCommittedAtInMillis", txCommittedAtInMillis)
          .add("type", type)
          .toString();
    }
  }

  public Record(
      RecordKey key,
      long version,
      @Nullable String currentTxId,
      boolean deleted,
      Set<Value> values,
      Set<String> insertTxIds) {
    this.key = key;
    this.version = version;
    this.currentTxId = currentTxId;
    this.deleted = deleted;
    this.values = values;
    this.insertTxIds = insertTxIds;
  }

  public long nextVersion() {
    return version + 1;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("version", version)
        .add("currentTxId", currentTxId)
        .add("deleted", deleted)
        .add("values", values)
        .add("insertTxIds", insertTxIds)
        .toString();
  }

  public String toStringOnlyWithMetadata() {
    return MoreObjects.toStringHelper(this)
        .add("key", key)
        .add("version", version)
        .add("currentTxId", currentTxId)
        .add("deleted", deleted)
        .add("values", Utils.convValuesToString(values))
        .add("insertTxIds", insertTxIds)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Record)) return false;
    Record record = (Record) o;
    return version == record.version && Objects.equals(key, record.key);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, version);
  }

  public static class RecordKey {
    public final String namespace;
    public final String table;
    public final Key pk;
    public final Key ck;

    public RecordKey(
        @JsonProperty("namespace") String namespace,
        @JsonProperty("table") String table,
        @JsonProperty("pk") Key pk,
        @JsonProperty("ck") Key ck) {
      this.namespace = namespace;
      this.table = table;
      this.pk = pk;
      this.ck = ck;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("namespace", namespace)
          .add("table", table)
          .add("pk", pk)
          .add("ck", ck)
          .toString();
    }
  }
}
