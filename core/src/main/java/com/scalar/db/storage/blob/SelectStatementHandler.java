package com.scalar.db.storage.blob;

import com.scalar.db.api.Get;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.EmptyScanner;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SelectStatementHandler extends StatementHandler {
  public SelectStatementHandler(BlobClientWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
  }

  @Nonnull
  public Scanner handle(Selection selection) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(selection);
    if (selection instanceof Get) {
      if (ScalarDbUtils.isSecondaryIndexSpecified(selection, tableMetadata)) {
        throw new ExecutionException(
            CoreError.BLOB_ERROR_OCCURRED_IN_SELECTION.buildMessage(
                "Get with secondary index is not supported"));
      } else {
        return executeGet((Get) selection, tableMetadata);
      }
    } else {
      if (selection instanceof ScanAll) {
        return executeScanAll((ScanAll) selection, tableMetadata);
      } else if (ScalarDbUtils.isSecondaryIndexSpecified(selection, tableMetadata)) {
        throw new ExecutionException(
            CoreError.BLOB_ERROR_OCCURRED_IN_SELECTION.buildMessage(
                "Scan with secondary index is not supported"));
      } else {
        return executeScan((Scan) selection, tableMetadata);
      }
    }
  }

  private Scanner executeGet(Get get, TableMetadata metadata) throws ExecutionException {
    BlobOperation operation = new BlobOperation(get, metadata);
    operation.checkArgument(Get.class);
    Optional<BlobRecord> record =
        getRecord(
            getNamespace(get),
            getTable(get),
            operation.getConcatenatedPartitionKey(),
            operation.getConcatenatedKey());
    if (!record.isPresent()) {
      return new EmptyScanner();
    }
    return new PointQueryScanner(
        record.get(), new ResultInterpreter(get.getProjections(), metadata));
  }

  private Scanner executeScan(Scan scan, TableMetadata metadata) throws ExecutionException {
    BlobOperation operation = new BlobOperation(scan, metadata);
    operation.checkArgument(Scan.class);
    List<BlobRecord> records =
        new ArrayList<>(
            getRecordsInPartition(
                getNamespace(scan), getTable(scan), operation.getConcatenatedPartitionKey()));

    records.sort(
        (o1, o2) ->
            new ClusteringKeyComparator(metadata)
                .compare(o1.getClusteringKey(), o2.getClusteringKey()));
    if (isReverseOrder(scan, metadata)) {
      Collections.reverse(records);
    }

    // If the scan is for DESC clustering order, use the end clustering key as a start key and the
    // start clustering key as an end key
    boolean scanForDescClusteringOrder = isScanForDescClusteringOrder(scan, metadata);
    Optional<Key> startKey =
        scanForDescClusteringOrder ? scan.getEndClusteringKey() : scan.getStartClusteringKey();
    boolean startInclusive =
        scanForDescClusteringOrder ? scan.getEndInclusive() : scan.getStartInclusive();
    Optional<Key> endKey =
        scanForDescClusteringOrder ? scan.getStartClusteringKey() : scan.getEndClusteringKey();
    boolean endInclusive =
        scanForDescClusteringOrder ? scan.getStartInclusive() : scan.getEndInclusive();

    if (startKey.isPresent()) {
      records =
          filterRecordsByClusteringKeyBoundary(
              records, startKey.get(), true, startInclusive, metadata);
    }
    if (endKey.isPresent()) {
      records =
          filterRecordsByClusteringKeyBoundary(
              records, endKey.get(), false, endInclusive, metadata);
    }

    if (scan.getLimit() > 0) {
      records = records.subList(0, Math.min(scan.getLimit(), records.size()));
    }

    return new RangeQueryScanner(
        records.iterator(),
        new ResultInterpreter(scan.getProjections(), metadata),
        scan.getLimit());
  }

  private Scanner executeScanAll(ScanAll scan, TableMetadata metadata) throws ExecutionException {
    BlobOperation operation = new BlobOperation(scan, metadata);
    operation.checkArgument(ScanAll.class);
    Set<BlobRecord> records = getRecordsInTable(getNamespace(scan), getTable(scan));
    if (scan.getLimit() > 0) {
      records = records.stream().limit(scan.getLimit()).collect(Collectors.toSet());
    }
    return new RangeQueryScanner(
        records.iterator(),
        new ResultInterpreter(scan.getProjections(), metadata),
        scan.getLimit());
  }

  private Optional<BlobRecord> getRecord(
      String namespace, String table, String partition, String concatenatedKey)
      throws ExecutionException {
    try {
      BlobClientWrapperResponse response =
          wrapper.get(BlobUtils.getObjectKey(namespace, table, partition, concatenatedKey));
      return Optional.of(JsonConvertor.deserialize(response.getValue(), BlobRecord.class));
    } catch (BlobClientWrapperException e) {
      if (e.getCode() == BlobClientWrapperException.StatusCode.NOT_FOUND) {
        return Optional.empty();
      } else {
        throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_SELECTION.buildMessage(), e);
      }
    } catch (Exception e) {
      throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_SELECTION.buildMessage(), e);
    }
  }

  private Set<BlobRecord> getRecordsInPartition(String namespace, String table, String partition)
      throws ExecutionException {
    Set<String> keys = wrapper.listKeys(BlobUtils.getObjectKey(namespace, table, partition, null));
    Set<BlobRecord> records = new HashSet<>();
    for (String key : keys) {
      try {
        BlobClientWrapperResponse response = wrapper.get(key);
        records.add(JsonConvertor.deserialize(response.getValue(), BlobRecord.class));
      } catch (BlobClientWrapperException e) {
        if (e.getCode() != BlobClientWrapperException.StatusCode.NOT_FOUND) {
          throw new ExecutionException(
              CoreError.BLOB_ERROR_OCCURRED_IN_SELECTION.buildMessage(), e);
        }
      } catch (Exception e) {
        throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_SELECTION.buildMessage(), e);
      }
    }
    return records;
  }

  private Set<BlobRecord> getRecordsInTable(String namespace, String table)
      throws ExecutionException {
    Set<String> keys = wrapper.listKeys(BlobUtils.getObjectKey(namespace, table, null, null));
    Set<BlobRecord> records = new HashSet<>();
    for (String key : keys) {
      try {
        BlobClientWrapperResponse response = wrapper.get(key);
        records.add(JsonConvertor.deserialize(response.getValue(), BlobRecord.class));
      } catch (BlobClientWrapperException e) {
        if (e.getCode() != BlobClientWrapperException.StatusCode.NOT_FOUND) {
          throw new ExecutionException(
              CoreError.BLOB_ERROR_OCCURRED_IN_SELECTION.buildMessage(), e);
        }
      } catch (Exception e) {
        throw new ExecutionException(CoreError.BLOB_ERROR_OCCURRED_IN_SELECTION.buildMessage(), e);
      }
    }
    return records;
  }

  private boolean isReverseOrder(Scan scan, TableMetadata metadata) {
    Boolean reverse = null;
    Iterator<String> iterator = metadata.getClusteringKeyNames().iterator();
    for (Scan.Ordering ordering : scan.getOrderings()) {
      String clusteringKeyName = iterator.next();
      if (!ordering.getColumnName().equals(clusteringKeyName)) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_ORDERING_NOT_PROPERLY_SPECIFIED.buildMessage(scan));
      }

      boolean rightOrder =
          ordering.getOrder() != metadata.getClusteringOrder(ordering.getColumnName());
      if (reverse == null) {
        reverse = rightOrder;
      } else {
        if (reverse != rightOrder) {
          throw new IllegalArgumentException(
              CoreError.OPERATION_CHECK_ERROR_ORDERING_NOT_PROPERLY_SPECIFIED.buildMessage(scan));
        }
      }
    }
    return reverse != null && reverse;
  }

  private boolean isScanForDescClusteringOrder(Scan scan, TableMetadata tableMetadata) {
    if (scan.getStartClusteringKey().isPresent()) {
      Key startClusteringKey = scan.getStartClusteringKey().get();
      String lastValueName =
          startClusteringKey.getColumns().get(startClusteringKey.size() - 1).getName();
      return tableMetadata.getClusteringOrder(lastValueName) == Scan.Ordering.Order.DESC;
    }
    if (scan.getEndClusteringKey().isPresent()) {
      Key endClusteringKey = scan.getEndClusteringKey().get();
      String lastValueName =
          endClusteringKey.getColumns().get(endClusteringKey.size() - 1).getName();
      return tableMetadata.getClusteringOrder(lastValueName) == Scan.Ordering.Order.DESC;
    }
    return false;
  }

  private List<BlobRecord> filterRecordsByClusteringKeyBoundary(
      List<BlobRecord> records,
      Key clusteringKey,
      boolean isStart,
      boolean isInclusive,
      TableMetadata metadata) {
    for (Column<?> column : clusteringKey.getColumns()) {
      Scan.Ordering.Order order = metadata.getClusteringOrder(column.getName());
      if (clusteringKey.getColumns().indexOf(column) == clusteringKey.size() - 1) {
        return records.stream()
            .filter(
                record -> {
                  Column<?> recordColumn =
                      ColumnValueMapper.convert(
                          record.getClusteringKey().get(column.getName()),
                          column.getName(),
                          column.getDataType());
                  int cmp =
                      new ColumnComparator(column.getDataType()).compare(recordColumn, column);
                  cmp = order == Scan.Ordering.Order.ASC ? cmp : -cmp;
                  if (isStart) {
                    if (isInclusive) {
                      return cmp >= 0;
                    } else {
                      return cmp > 0;
                    }
                  } else {
                    if (isInclusive) {
                      return cmp <= 0;
                    } else {
                      return cmp < 0;
                    }
                  }
                })
            .collect(Collectors.toList());
      } else {
        List<BlobRecord> tmpRecords = new ArrayList<>();
        records.forEach(
            record -> {
              Column<?> recordColumn =
                  ColumnValueMapper.convert(
                      record.getClusteringKey().get(column.getName()),
                      column.getName(),
                      column.getDataType());
              int cmp = new ColumnComparator(column.getDataType()).compare(recordColumn, column);
              if (cmp == 0) {
                tmpRecords.add(record);
              }
            });
        if (tmpRecords.isEmpty()) {
          return Collections.emptyList();
        }
        records = tmpRecords;
      }
    }
    return records;
  }
}
