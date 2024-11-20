package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.time.Instant;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;

/** A {@code Column} for a TimestampTZ type. */
public class TimestampTZColumn implements Column<Instant> {
  private final String name;
  @Nullable private final Instant value;

  private TimestampTZColumn(String name, @Nullable Instant value) {
    this.name = Objects.requireNonNull(name);
    this.value = value;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<Instant> getValue() {
    return Optional.ofNullable(value);
  }

  @Nullable
  @Override
  public Instant getTimestampTZValue() {
    return value;
  }

  @Override
  public TimestampTZColumn copyWith(String name) {
    return new TimestampTZColumn(name, value);
  }

  @Override
  public DataType getDataType() {
    return DataType.TIMESTAMPTZ;
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
  public int compareTo(Column<Instant> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(value, o.getTimestampTZValue(), Comparator.nullsFirst(Comparator.naturalOrder()))
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TimestampTZColumn)) {
      return false;
    }
    TimestampTZColumn that = (TimestampTZColumn) o;
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
   * Returns a TimestampTZ column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a TimestampTZ column instance with the specified column name and value
   */
  public static TimestampTZColumn of(String columnName, Instant value) {
    return new TimestampTZColumn(columnName, value);
  }

  /**
   * Returns a TimestampTZ column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a TimestampTZ column instance with the specified column name and a null value
   */
  public static TimestampTZColumn ofNull(String columnName) {
    return new TimestampTZColumn(columnName, null);
  }
}
