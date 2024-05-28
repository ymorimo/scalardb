package com.scalar.db.dataloader.cli.command;

import static org.junit.jupiter.api.Assertions.*;

import com.scalar.db.api.Scan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ScanOrderingConverterTest {

  private final ScanOrderingConverter converter = new ScanOrderingConverter();

  @ParameterizedTest
  @CsvSource({"age, ASC", "name, DESC", "score, asc", "RANK, desc"})
  void convert_ValidInput_ReturnsScanOrdering(String columnName, String order) {
    // Act
    Scan.Ordering result = converter.convert(columnName + "=" + order);

    // Assert
    assertEquals(columnName, result.getColumnName());
    assertEquals(Scan.Ordering.Order.valueOf(order.toUpperCase()), result.getOrder());
  }

  @ParameterizedTest
  @ValueSource(strings = {"age", "name=", "=DESC", "score=INVALID", "RANK=ascDESC"})
  void convert_InvalidInput_ThrowsIllegalArgumentException(String input) {
    // Act & Assert
    assertThrows(IllegalArgumentException.class, () -> converter.convert(input));
  }

  @Test
  void convert_NullInput_ThrowsIllegalArgumentException() {
    // Act & Assert
    assertThrows(IllegalArgumentException.class, () -> converter.convert(null));
  }

  @Test
  void convert_EmptyColumnName_ThrowsIllegalArgumentException() {
    // Act & Assert
    assertThrows(IllegalArgumentException.class, () -> converter.convert("=ASC"));
  }

  @Test
  void convert_EmptyOrder_ThrowsIllegalArgumentException() {
    // Act & Assert
    assertThrows(IllegalArgumentException.class, () -> converter.convert("age="));
  }

  @ParameterizedTest
  @ValueSource(strings = {"asc", "desc", "ASC", "DESC"})
  void convert_ValidOrder_ReturnsScanOrdering(String order) {
    // Arrange
    String columnName = "age";

    // Act
    Scan.Ordering result = converter.convert(columnName + "=" + order);

    // Assert
    assertEquals(columnName, result.getColumnName());
    assertEquals(Scan.Ordering.Order.valueOf(order.toUpperCase()), result.getOrder());
  }

  @ParameterizedTest
  @ValueSource(strings = {"ascending", "descending", "up", "down"})
  void convert_InvalidOrder_ThrowsIllegalArgumentException(String order) {
    // Arrange
    String columnName = "age";

    // Act & Assert
    assertThrows(IllegalArgumentException.class, () -> converter.convert(columnName + "=" + order));
  }
}
