package com.scalar.db.storage.objectstorage;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;

public class ObjectStorageOperationChecker extends OperationChecker {
  private static final char[] ILLEGAL_CHARACTERS_IN_PRIMARY_KEY = {
    '/',
    // TODO: Add more illegal characters
  };

  public ObjectStorageOperationChecker(
      DatabaseConfig databaseConfig, TableMetadataManager metadataManager) {
    super(databaseConfig, metadataManager);
  }

  @Override
  public void check(Put put) throws ExecutionException {
    super.check(put);
    TableMetadata metadata = getTableMetadata(put);
    checkCondition(put, metadata);
  }

  @Override
  public void check(Delete delete) throws ExecutionException {
    super.check(delete);
    TableMetadata metadata = getTableMetadata(delete);
    checkCondition(delete, metadata);
  }

  private void checkCondition(Mutation mutation, TableMetadata metadata) {
    // TODO: Implement the condition check
  }
}
