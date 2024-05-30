package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

public class ReplicationGroupCommitException extends Exception {
  public ReplicationGroupCommitException(String message) {
    super(message);
  }

  public ReplicationGroupCommitException(String message, Throwable cause) {
    super(message, cause);
  }
}
