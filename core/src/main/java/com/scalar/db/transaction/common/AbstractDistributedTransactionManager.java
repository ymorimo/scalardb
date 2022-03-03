package com.scalar.db.transaction.common;

import com.scalar.db.api.DistributedTransactionManager;
import java.util.Optional;

public abstract class AbstractDistributedTransactionManager
    implements DistributedTransactionManager {

  private Optional<String> namespace;
  private Optional<String> tableName;

  public AbstractDistributedTransactionManager() {
    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  @Deprecated
  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  @Deprecated
  @Override
  public Optional<String> getTable() {
    return tableName;
  }
}
