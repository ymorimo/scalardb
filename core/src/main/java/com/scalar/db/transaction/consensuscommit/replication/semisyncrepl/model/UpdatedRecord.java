package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import java.time.Instant;

public class UpdatedRecord {
  public final int partitionId;
  public final String namespace;
  public final String table;
  public final Key pk;
  public final Key ck;
  public final Instant updatedAt;

  public UpdatedRecord(
      int partitionId, String namespace, String table, Key pk, Key ck, Instant updatedAt) {
    this.partitionId = partitionId;
    this.namespace = namespace;
    this.table = table;
    this.pk = pk;
    this.ck = ck;
    this.updatedAt = updatedAt;
  }
}
