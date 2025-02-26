package com.scalar.db.storage.blob;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Operation;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import java.util.List;
import javax.annotation.Nonnull;

public class StatementHandler {
  protected final BlobClientWrapper wrapper;
  protected final TableMetadataManager metadataManager;

  protected StatementHandler(BlobClientWrapper wrapper, TableMetadataManager metadataManager) {
    this.wrapper = wrapper;
    this.metadataManager = metadataManager;
  }

  @Nonnull
  protected String getNamespace(Operation operation) {
    assert operation.forNamespace().isPresent();
    return operation.forNamespace().get();
  }

  @Nonnull
  protected String getTable(Operation operation) {
    assert operation.forTable().isPresent();
    return operation.forTable().get();
  }

  protected boolean areConditionsMet(
      BlobRecord record, List<ConditionalExpression> expressions, TableMetadata metadata) {
    for (ConditionalExpression expression : expressions) {
      Column<?> expectedColumn = expression.getColumn();
      Column<?> actualColumn =
          ColumnValueMapper.convert(
              record.getValues().get(expectedColumn.getName()),
              expectedColumn.getName(),
              metadata.getColumnDataType(expectedColumn.getName()));
      DataType dataType = metadata.getColumnDataType(expectedColumn.getName());
      switch (expression.getOperator()) {
        case EQ:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (new ColumnComparator(dataType).compare(actualColumn, expectedColumn) != 0) {
            return false;
          }
          break;
        case NE:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (new ColumnComparator(dataType).compare(actualColumn, expectedColumn) == 0) {
            return false;
          }
          break;
        case GT:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (new ColumnComparator(dataType).compare(actualColumn, expectedColumn) <= 0) {
            return false;
          }
          break;
        case GTE:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (new ColumnComparator(dataType).compare(actualColumn, expectedColumn) < 0) {
            return false;
          }
          break;
        case LT:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (new ColumnComparator(dataType).compare(actualColumn, expectedColumn) >= 0) {
            return false;
          }
          break;
        case LTE:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          if (new ColumnComparator(dataType).compare(actualColumn, expectedColumn) > 0) {
            return false;
          }
          break;
        case IS_NULL:
          if (!actualColumn.hasNullValue()) {
            return false;
          }
          break;
        case IS_NOT_NULL:
          if (actualColumn.hasNullValue()) {
            return false;
          }
          break;
        default:
          throw new AssertionError("Unsupported operator"); // TODO: Implement other operators
      }
    }
    return true;
  }
}
