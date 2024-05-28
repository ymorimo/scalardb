package com.scalar.db.dataloader.core;

import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import lombok.Builder;
import lombok.Value;

/** Parameters for a ScalarDB scan operation */
@Builder
@Value
@SuppressWarnings({"SameNameButDifferent", "MissingSummary"})
@SuppressFBWarnings({"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
public class ScanParameters {
  String namespace;
  String tableName;
  Key partitionKey;
  Key scanStartKey;
  Key scanEndKey;
  boolean isStartInclusive;
  boolean isEndInclusive;
  List<String> projectionColumns;
  List<Scan.Ordering> sortOrders;
  int limit;
}
