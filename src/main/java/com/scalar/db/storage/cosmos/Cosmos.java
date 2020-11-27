package com.scalar.db.storage.cosmos;

import static com.google.common.base.Preconditions.checkArgument;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.Utility;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A storage implementation with Cosmos DB for {@link DistributedStorage}
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class Cosmos implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(Cosmos.class);
  private final String METADATA_DATABASE = "scalardb";
  private final String METADATA_CONTAINER = "metadata";

  private final CosmosClient client;
  private final TableMetadataManager metadataManager;
  private final SelectStatementHandler selectStatementHandler;
  private final PutStatementHandler putStatementHandler;
  private final DeleteStatementHandler deleteStatementHandler;
  private final BatchHandler batchHandler;
  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public Cosmos(DatabaseConfig config) {
    this.client =
        new CosmosClientBuilder()
            .endpoint(config.getContactPoints().get(0))
            .key(config.getPassword())
            .directMode()
            .consistencyLevel(ConsistencyLevel.STRONG)
            .buildClient();

    CosmosContainer container =
        client.getDatabase(METADATA_DATABASE).getContainer(METADATA_CONTAINER);
    this.metadataManager = new TableMetadataManager(container);

    this.selectStatementHandler = new SelectStatementHandler(client, metadataManager);
    this.putStatementHandler = new PutStatementHandler(client, metadataManager);
    this.deleteStatementHandler = new DeleteStatementHandler(client, metadataManager);
    this.batchHandler = new BatchHandler(client, metadataManager);

    LOGGER.info("Cosmos DB object is created properly.");

    namespace = Optional.empty();
    tableName = Optional.empty();
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  @Override
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    Utility.setTargetToIfNot(get, namespace, tableName);

    List<Record> records = selectStatementHandler.handle(get);

    if (records.isEmpty() || records.get(0) == null) {
      return Optional.empty();
    }

    CosmosTableMetadata metadata = metadataManager.getTableMetadata(get);
    return Optional.of(new ResultImpl(records.get(0), get, metadata));
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    Utility.setTargetToIfNot(scan, namespace, tableName);

    List<Record> records = selectStatementHandler.handle(scan);

    CosmosTableMetadata metadata = metadataManager.getTableMetadata(scan);
    return new ScannerImpl(records, scan, metadata);
  }

  @Override
  public void put(Put put) throws ExecutionException {
    Utility.setTargetToIfNot(put, namespace, tableName);
    checkIfPrimaryKeyExists(put);

    putStatementHandler.handle(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    Utility.setTargetToIfNot(delete, namespace, tableName);
    deleteStatementHandler.handle(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    checkArgument(mutations.size() != 0);
    if (mutations.size() > 1) {
      Utility.setTargetToIfNot(mutations, namespace, tableName);
      batchHandler.handle(mutations);
    } else if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private void checkIfPrimaryKeyExists(Put put) {
    CosmosTableMetadata metadata = metadataManager.getTableMetadata(put);

    Utility.checkIfPrimaryKeyExists(put, metadata);
  }
}
