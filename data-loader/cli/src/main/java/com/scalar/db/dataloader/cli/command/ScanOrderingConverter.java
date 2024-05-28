package com.scalar.db.dataloader.cli.command;

import com.scalar.db.api.Scan;
import picocli.CommandLine;

/**
 * Converts a string representation of a column ordering into a {@link Scan.Ordering} object. The
 * string format should be "columnName=order", where order is either "ASC" or "DESC".
 */
public class ScanOrderingConverter implements CommandLine.ITypeConverter<Scan.Ordering> {

  private static final String ASC = "ASC";
  private static final String DESC = "DESC";

  /**
   * Converts a string representation of a column ordering into a {@link Scan.Ordering} object.
   *
   * @param value the string representation of the column ordering in the format "columnName=order"
   * @return a {@link Scan.Ordering} object representing the column ordering
   * @throws IllegalArgumentException if the input string is not in the expected format or if the
   *     order is invalid
   */
  @Override
  public Scan.Ordering convert(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Column ordering cannot be null");
    }
    String[] parts = value.split("=", 2);
    if (parts.length != 2) {
      throw new IllegalArgumentException("Invalid column ordering format: " + value);
    }
    String columnName = parts[0].trim();
    String order = parts[1].trim().toUpperCase();

    // Column name cannot be empty
    if (columnName.isEmpty()) {
      throw new IllegalArgumentException("Column name cannot be empty");
    }

    // Order cannot be empty
    if (order.isEmpty()) {
      throw new IllegalArgumentException("Order cannot be empty");
    }

    // Order must be either "ASC" or "DESC"
    if (!order.equals(ASC) && !order.equals(DESC)) {
      throw new IllegalArgumentException("Invalid order: " + order);
    }
    // Return the Scan.Ordering object
    return order.equals(ASC) ? Scan.Ordering.asc(columnName) : Scan.Ordering.desc(columnName);
  }
}
