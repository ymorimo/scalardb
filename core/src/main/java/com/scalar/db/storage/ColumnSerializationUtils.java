package com.scalar.db.storage;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import com.google.common.base.Strings;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;

public final class ColumnSerializationUtils {

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .append(BASIC_ISO_DATE)
          .appendValue(HOUR_OF_DAY, 2)
          .appendValue(MINUTE_OF_HOUR, 2)
          .appendValue(SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(NANO_OF_SECOND, 0, 9, false)
          .toFormatter()
          .withChronology(IsoChronology.INSTANCE)
          .withResolverStyle(ResolverStyle.STRICT);

  private ColumnSerializationUtils() {}
  // TODO add comments on all methods
  public static String toJDBCFormat(DateColumn column) {
    assert column.getDateValue() != null;
    return DateTimeFormatter.ISO_LOCAL_DATE.format(column.getDateValue());
  }

  public static String toJDBCFormat(TimeColumn column) {
    assert column.getTimeValue() != null;
    return DateTimeFormatter.ISO_LOCAL_TIME.format(column.getTimeValue());
  }

  public static String toJDBCFormat(TimestampColumn column) {
    assert column.getTimestampValue() != null;
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(column.getTimestampValue());
  }

  public static String toJDBCFormat(TimestampTZColumn column) {
    assert column.getTimestampTZValue() != null;
    return DateTimeFormatter.ISO_INSTANT.format(column.getTimestampTZValue());
  }

  public static long toCompactFormat(DateColumn column) {
    assert column.getDateValue() != null;
    return column.getDateValue().toEpochDay();
  }

  public static long toCompactFormat(TimeColumn column) {
    assert column.getTimeValue() != null;
    return column.getTimeValue().toNanoOfDay();
  }

  public static String toCompactFormat(TimestampColumn column) {
    assert column.getTimestampValue() != null;
    return TIMESTAMP_FORMATTER.format(column.getTimestampValue());
  }

  public static String toCompactFormat(TimestampTZColumn column) {
    assert column.getTimestampTZValue() != null;
    return TIMESTAMP_FORMATTER.format(column.getTimestampTZValue().atOffset(ZoneOffset.UTC));
  }

  public static LocalDate parseCompactDate(long epochDay) {
    return LocalDate.ofEpochDay(epochDay);
  }

  public static LocalTime parseCompactTime(long nanoOfDay) {
    return LocalTime.ofNanoOfDay(nanoOfDay);
  }

  public static LocalDateTime parseCompactTimestamp(String text) {
    return Strings.isNullOrEmpty(text)
        ? null
        : TIMESTAMP_FORMATTER.parse(text, LocalDateTime::from);
  }

  public static Instant parseCompactTimestampTZ(String text) {
    return Strings.isNullOrEmpty(text) ? null : TIMESTAMP_FORMATTER.parse(text, Instant::from);
  }
}
