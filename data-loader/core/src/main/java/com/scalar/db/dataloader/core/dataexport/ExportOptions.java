package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.api.Scan;
import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/** Options for a ScalarDB export data operation */
@Value
@Builder(builderMethodName = "internalBuilder")
@SuppressWarnings("SameNameButDifferent")
public class ExportOptions {

  // Fields with no default values
  String namespace;
  String tableName;
  FileFormat outputFileFormat;
  Key scanPartitionKey;
  Key scanStartKey;
  Key scanEndKey;
  boolean isStartInclusive;
  boolean isEndInclusive;
  int limit;
  int maxThreadCount;
  boolean prettyPrintJson;
  List<Scan.Ordering> sortOrders;

  // Fields with default values
  @Builder.Default int dataChunkSize = Integer.MAX_VALUE;
  @Builder.Default String delimiter = ",";
  @Builder.Default boolean excludeHeaderRow = false;
  @Builder.Default boolean includeTransactionMetadata = false;
  @Builder.Default List<String> projectionColumns = Collections.emptyList();

  /**
   * Custom builder for ExportOptions including the minimum required fields
   *
   * @param namespace ScalarDB namespace
   * @param tableName ScalarDB table name
   * @param scanPartitionKey ScalarDB partition key
   * @param outputFileFormat File format for the output file
   * @return ExportOptionsBuilder
   */
  public static ExportOptionsBuilder builder(
      String namespace, String tableName, Key scanPartitionKey, FileFormat outputFileFormat) {
    return internalBuilder()
        .namespace(namespace)
        .tableName(tableName)
        .scanPartitionKey(scanPartitionKey)
        .outputFileFormat(outputFileFormat);
  }
}
