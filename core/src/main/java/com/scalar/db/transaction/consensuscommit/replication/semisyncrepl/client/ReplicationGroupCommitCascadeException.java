package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.client;

public class ReplicationGroupCommitCascadeException extends ReplicationGroupCommitException {
  public ReplicationGroupCommitCascadeException(String message) {
    super(message);
  }

  public ReplicationGroupCommitCascadeException(String message, Throwable cause) {
    super(message, cause);
  }
}
