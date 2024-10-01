package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/** A {@code Column} for a DATETIME type. */
public class DateTimeColumn implements Column<LocalDateTime> {

  private final String name;
  @Nullable private final LocalDateTime value;

  private DateTimeColumn(String name, @Nullable LocalDateTime value) {
    this.name = Objects.requireNonNull(name);
    this.value = value;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<LocalDateTime> getValue() {
    return Optional.ofNullable(value);
  }

  @Nullable
  @Override
  public LocalDateTime getDateTimeValue() {
    return value;
  }

  @Override
  public DateTimeColumn copyWith(String name) {
    return new DateTimeColumn(name, value);
  }

  @Override
  public DataType getDataType() {
    return DataType.DATETIME;
  }

  @Override
  public boolean hasNullValue() {
    return value == null;
  }

  @Nullable
  @Override
  public Object getValueAsObject() {
    return value;
  }

  @Override
  public int compareTo(Column<LocalDateTime> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(value, o.getDateTimeValue(), Comparator.nullsFirst(Comparator.naturalOrder()))
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DateTimeColumn)) {
      return false;
    }
    DateTimeColumn that = (DateTimeColumn) o;
    return Objects.equals(name, that.name) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  @Override
  public void accept(ColumnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }
  /**
   * Returns a Datetime column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a Datetime column instance with the specified column name and value
   */
  public static DateTimeColumn of(String columnName, LocalDateTime value) {
    return new DateTimeColumn(columnName, value);
  }

  /**
   * Returns a Datetime column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a Datetime column instance with the specified column name and a null value
   */
  public static DateTimeColumn ofNull(String columnName) {
    return new DateTimeColumn(columnName, null);
  }
}
