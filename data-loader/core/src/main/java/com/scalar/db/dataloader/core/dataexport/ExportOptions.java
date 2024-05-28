package com.scalar.db.dataloader.core.dataexport;

import com.scalar.db.dataloader.core.FileFormat;
import com.scalar.db.dataloader.core.ScanParameters;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Builder;
import lombok.Value;

/** Options for a ScalarDB export data operation */
@Value
@Builder(builderMethodName = "internalBuilder")
@SuppressWarnings({"SameNameButDifferent", "MissingSummary"})
@SuppressFBWarnings({"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
public class ExportOptions {

  // Fields without default values
  ScanParameters scanParameters;
  String namespace;
  String tableName;
  FileFormat outputFileFormat;
  int maxThreadCount;
  boolean prettyPrintJson;

  // Fields with default values
  @Builder.Default int dataChunkSize = 0;
  @Builder.Default String csvDelimiter = ",";
  @Builder.Default boolean includeHeaderRow = true;
  @Builder.Default boolean includeTransactionMetadata = false;

  /**
   * Custom builder for ExportOptions including the minimum required fields
   *
   * @param scanParameters ScalarDB scan parameters
   * @param outputFileFormat File format for the output file
   * @return ExportOptionsBuilder
   */
  public static ExportOptionsBuilder builder(
      ScanParameters scanParameters, FileFormat outputFileFormat) {
    return internalBuilder().scanParameters(scanParameters).outputFileFormat(outputFileFormat);
  }
}
