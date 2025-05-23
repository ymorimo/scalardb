package com.scalar.db.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetWithIndex;
import com.scalar.db.api.Insert;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.ScanWithIndex;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ReferenceEquality")
public class ScalarDbUtilsTest {

  private static final Optional<String> NAMESPACE = Optional.of("ns");
  private static final Optional<String> TABLE = Optional.of("tbl");

  @Test
  public void copyAndSetTargetToIfNot_GetGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Get get = Get.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Get actual = ScalarDbUtils.copyAndSetTargetToIfNot(get, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == get).isFalse();
    assertThat(actual instanceof GetWithIndex).isFalse();
    assertThat(get.forNamespace()).isNotPresent();
    assertThat(get.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_GetWithIndexGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Get getWithIndex = Get.newBuilder().table(TABLE.get()).indexKey(Key.ofText("c1", "v1")).build();

    // Act
    Get actual = ScalarDbUtils.copyAndSetTargetToIfNot(getWithIndex, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == getWithIndex).isFalse();
    assertThat(actual instanceof GetWithIndex).isTrue();
    assertThat(getWithIndex.forNamespace()).isNotPresent();
    assertThat(getWithIndex.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_ScanGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Scan scan = Scan.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Scan actual = ScalarDbUtils.copyAndSetTargetToIfNot(scan, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == scan).isFalse();
    assertThat(actual instanceof ScanWithIndex).isFalse();
    assertThat(actual instanceof ScanAll).isFalse();
    assertThat(scan.forNamespace()).isNotPresent();
    assertThat(scan.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_ScanAllGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Scan scanAll = Scan.newBuilder().table(TABLE.get()).all().build();

    // Act
    Scan actual = ScalarDbUtils.copyAndSetTargetToIfNot(scanAll, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == scanAll).isFalse();
    assertThat(actual instanceof ScanAll).isTrue();
    assertThat(scanAll.forNamespace()).isNotPresent();
    assertThat(scanAll.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_ScanWithIndexGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Scan scanWithIndex =
        Scan.newBuilder().table(TABLE.get()).indexKey(Key.ofText("c1", "v1")).build();

    // Act
    Scan actual = ScalarDbUtils.copyAndSetTargetToIfNot(scanWithIndex, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == scanWithIndex).isFalse();
    assertThat(actual instanceof ScanWithIndex).isTrue();
    assertThat(scanWithIndex.forNamespace()).isNotPresent();
    assertThat(scanWithIndex.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_PutGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Put put = Put.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Put actual = ScalarDbUtils.copyAndSetTargetToIfNot(put, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == put).isFalse();
    assertThat(put.forNamespace()).isNotPresent();
    assertThat(put.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_DeleteGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Delete delete =
        Delete.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Delete actual = ScalarDbUtils.copyAndSetTargetToIfNot(delete, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == delete).isFalse();
    assertThat(delete.forNamespace()).isNotPresent();
    assertThat(delete.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_InsertGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Insert insert =
        Insert.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Insert actual = ScalarDbUtils.copyAndSetTargetToIfNot(insert, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == insert).isFalse();
    assertThat(insert.forNamespace()).isNotPresent();
    assertThat(insert.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_UpsertGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Upsert upsert =
        Upsert.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Upsert actual = ScalarDbUtils.copyAndSetTargetToIfNot(upsert, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == upsert).isFalse();
    assertThat(upsert.forNamespace()).isNotPresent();
    assertThat(upsert.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_UpdateGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Update update =
        Update.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();

    // Act
    Update actual = ScalarDbUtils.copyAndSetTargetToIfNot(update, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == update).isFalse();
    assertThat(update.forNamespace()).isNotPresent();
    assertThat(update.forTable()).isEqualTo(TABLE);
    assertThat(actual.forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.forTable()).isEqualTo(TABLE);
  }

  @Test
  public void copyAndSetTargetToIfNot_MutationsGiven_ShouldReturnDifferentInstance() {
    // Arrange
    Put put = Put.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();
    Delete delete =
        Delete.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();
    Insert insert =
        Insert.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();
    Upsert upsert =
        Upsert.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();
    Update update =
        Update.newBuilder().table(TABLE.get()).partitionKey(Key.ofText("c1", "v1")).build();
    List<Mutation> mutations = Arrays.asList(put, delete, insert, upsert, update);

    // Act
    List<Mutation> actual = ScalarDbUtils.copyAndSetTargetToIfNot(mutations, NAMESPACE, TABLE);

    // Assert
    assertThat(actual == mutations).isFalse();
    assertThat(actual.get(0) == put).isFalse();
    assertThat(actual.get(1) == delete).isFalse();
    assertThat(actual.get(2) == insert).isFalse();
    assertThat(actual.get(3) == upsert).isFalse();
    assertThat(actual.get(4) == update).isFalse();
    assertThat(put.forNamespace()).isNotPresent();
    assertThat(put.forTable()).isEqualTo(TABLE);
    assertThat(delete.forNamespace()).isNotPresent();
    assertThat(delete.forTable()).isEqualTo(TABLE);
    assertThat(insert.forNamespace()).isNotPresent();
    assertThat(insert.forTable()).isEqualTo(TABLE);
    assertThat(upsert.forNamespace()).isNotPresent();
    assertThat(upsert.forTable()).isEqualTo(TABLE);
    assertThat(update.forNamespace()).isNotPresent();
    assertThat(update.forTable()).isEqualTo(TABLE);
    assertThat(actual.get(0).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(0).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(1).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(1).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(2).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(2).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(3).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(3).forTable()).isEqualTo(TABLE);
    assertThat(actual.get(4).forNamespace()).isEqualTo(NAMESPACE);
    assertThat(actual.get(4).forTable()).isEqualTo(TABLE);
  }

  @Test
  public void checkUpdate_ShouldBehaveProperly() {
    // Arrange
    Update updateWithValidCondition1 =
        Update.newBuilder()
            .namespace(NAMESPACE.get())
            .table(TABLE.get())
            .partitionKey(Key.ofText("c1", "v1"))
            .condition(
                ConditionBuilder.updateIf(ConditionBuilder.column("c2").isEqualToText("v2"))
                    .build())
            .build();
    Update updateWithValidCondition2 =
        Update.newBuilder()
            .namespace(NAMESPACE.get())
            .table(TABLE.get())
            .partitionKey(Key.ofText("c1", "v1"))
            .condition(ConditionBuilder.updateIfExists())
            .build();
    Update updateWithInvalidCondition =
        Update.newBuilder()
            .namespace(NAMESPACE.get())
            .table(TABLE.get())
            .partitionKey(Key.ofText("c1", "v1"))
            .condition(ConditionBuilder.putIfExists())
            .build();

    // Act
    assertThatCode(() -> ScalarDbUtils.checkUpdate(updateWithValidCondition1))
        .doesNotThrowAnyException();
    assertThatCode(() -> ScalarDbUtils.checkUpdate(updateWithValidCondition2))
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> ScalarDbUtils.checkUpdate(updateWithInvalidCondition))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void isMatchedWith_SomePatternsWithoutEscapeGiven_ShouldReturnBooleanProperly() {
    // Arrange Act Assert
    // The following tests are added referring to the similar tests in Spark.
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/RegexpExpressionsSuite.scala
    // simple patterns
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("abdef", prepareLike("abdef"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a_%b", prepareLike("a\\__b"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("addb", prepareLike("a_%b"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("addb", prepareLike("a\\__b"))).isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("addb", prepareLike("a%\\%b"))).isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a_%b", prepareLike("a%\\%b"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("addb", prepareLike("a%"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("addb", prepareLike("**"))).isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("abc", prepareLike("a%"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("abc", prepareLike("b%"))).isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("abc", prepareLike("bc%"))).isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a\nb", prepareLike("a_b"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("ab", prepareLike("a%b"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a\nb", prepareLike("a%b"))).isTrue();

    // empty input
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("", prepareLike(""))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a", prepareLike(""))).isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("", prepareLike("a"))).isFalse();

    // SI-17647 double-escaping backslash
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("\\\\\\\\", prepareLike("%\\\\%")))
        .isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("%%", prepareLike("%%"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("\\__", prepareLike("\\\\\\__"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("\\\\\\__", prepareLike("%\\\\%\\%")))
        .isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("_\\\\\\%", prepareLike("%\\\\")))
        .isFalse();

    // unicode
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a\u20ACa", prepareLike("_\u20AC_")))
        .isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a€a", prepareLike("_€_"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a€a", prepareLike("_\u20AC_"))).isTrue();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a\u20ACa", prepareLike("_€_"))).isTrue();

    // case
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("A", prepareLike("a%"))).isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a", prepareLike("A%"))).isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("AaA", prepareLike("_a_"))).isTrue();

    // example
    assertThat(
            ScalarDbUtils.stringMatchesLikeExpression(
                "%SystemDrive%\\Users\\John", prepareLike("\\%SystemDrive\\%\\\\Users%")))
        .isTrue();
  }

  @Test
  public void isMatchedWith_SomePatternsWithEscapeGiven_ShouldReturnBooleanProperly() {
    // Arrange Act Assert
    // The following tests are added referring to the similar tests in Spark.
    // https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/RegexpExpressionsSuite.scala
    ImmutableList.of("/", "#", "\"")
        .forEach(
            escape -> {
              // simple patterns
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "abdef", prepareLike("abdef", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "a_%b", prepareLike("a" + escape + "__b", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "addb", prepareLike("a_%b", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "addb", prepareLike("a" + escape + "__b", escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "addb", prepareLike("a%" + escape + "%b", escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "a_%b", prepareLike("a%" + escape + "%b", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("addb", prepareLike("a%", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("addb", prepareLike("**", escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("abc", prepareLike("a%", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("abc", prepareLike("b%", escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("abc", prepareLike("bc%", escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("a\nb", prepareLike("a_b", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("ab", prepareLike("a%b", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("a\nb", prepareLike("a%b", escape)))
                  .isTrue();

              // empty input
              assertThat(ScalarDbUtils.stringMatchesLikeExpression("", prepareLike("", escape)))
                  .isTrue();
              assertThat(ScalarDbUtils.stringMatchesLikeExpression("a", prepareLike("", escape)))
                  .isFalse();
              assertThat(ScalarDbUtils.stringMatchesLikeExpression("", prepareLike("a", escape)))
                  .isFalse();

              // SI-17647 double-escaping backslash
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          String.format("%s%s%s%s", escape, escape, escape, escape),
                          prepareLike(String.format("%%%s%s%%", escape, escape), escape)))
                  .isTrue();
              assertThat(ScalarDbUtils.stringMatchesLikeExpression("%%", prepareLike("%%", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          String.format("%s__", escape),
                          prepareLike(String.format("%s%s%s__", escape, escape, escape), escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          String.format("%s%s%s__", escape, escape, escape),
                          prepareLike(
                              String.format("%%%s%s%%%s%%", escape, escape, escape), escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          String.format("_%s%s%s%%", escape, escape, escape),
                          prepareLike(String.format("%%%s%s", escape, escape), escape)))
                  .isFalse();

              // unicode
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "a\u20ACa", prepareLike("_\u20AC_", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("a€a", prepareLike("_€_", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "a€a", prepareLike("_\u20AC_", escape)))
                  .isTrue();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "a\u20ACa", prepareLike("_€_", escape)))
                  .isTrue();

              // case
              assertThat(ScalarDbUtils.stringMatchesLikeExpression("A", prepareLike("a%", escape)))
                  .isFalse();
              assertThat(ScalarDbUtils.stringMatchesLikeExpression("a", prepareLike("A%", escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression("AaA", prepareLike("_a_", escape)))
                  .isTrue();

              // example
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          String.format("%%SystemDrive%%%sUsers%sJohn", escape, escape),
                          prepareLike(
                              String.format(
                                  "%s%%SystemDrive%s%%%s%sUsers%%", escape, escape, escape, escape),
                              escape)))
                  .isTrue();
            });
  }

  @Test
  public void isMatchedWith_IsNotLikeOperatorWithSomePatternsGiven_ShouldReturnBooleanProperly() {
    // Arrange Act Assert
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("abdef", prepareNotLike("abdef")))
        .isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("a_%b", prepareNotLike("a\\__b")))
        .isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("addb", prepareNotLike("a_%b"))).isFalse();
    assertThat(ScalarDbUtils.stringMatchesLikeExpression("addb", prepareNotLike("a\\__b")))
        .isTrue();
    ImmutableList.of("/", "#", "\"")
        .forEach(
            escape -> {
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "abdef", prepareNotLike("abdef", escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "a_%b", prepareNotLike("a" + escape + "__b", escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "addb", prepareNotLike("a_%b", escape)))
                  .isFalse();
              assertThat(
                      ScalarDbUtils.stringMatchesLikeExpression(
                          "addb", prepareNotLike("a" + escape + "__b", escape)))
                  .isTrue();
            });
  }

  private LikeExpression prepareLike(String pattern) {
    return ConditionBuilder.column("col1").isLikeText(pattern);
  }

  private LikeExpression prepareLike(String pattern, String escape) {
    return ConditionBuilder.column("col1").isLikeText(pattern, escape);
  }

  private LikeExpression prepareNotLike(String pattern) {
    return ConditionBuilder.column("col1").isNotLikeText(pattern);
  }

  private LikeExpression prepareNotLike(String pattern, String escape) {
    return ConditionBuilder.column("col1").isNotLikeText(pattern, escape);
  }

  @Test
  public void getPartitionKey_ShouldReturnPartitionKey() {
    // Arrange
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.INT)
            .addColumn("c3", DataType.INT)
            .addColumn("c4", DataType.BIGINT)
            .addColumn("c5", DataType.DOUBLE)
            .addPartitionKey("c1")
            .addPartitionKey("c2")
            .addClusteringKey("c3")
            .addClusteringKey("c4")
            .build();

    Result result =
        new ResultImpl(
            ImmutableMap.of(
                "c1", TextColumn.of("c1", "v1"),
                "c2", IntColumn.of("c2", 2),
                "c3", IntColumn.of("c3", 3),
                "c4", BigIntColumn.of("c4", 4L),
                "c5", DoubleColumn.of("c5", 5.0)),
            tableMetadata);

    // Act
    Key actual = ScalarDbUtils.getPartitionKey(result, tableMetadata);

    // Assert
    assertThat(actual.getColumns().size()).isEqualTo(2);
    assertThat(actual.getColumns().get(0)).isInstanceOf(TextColumn.class);
    assertThat(actual.getColumns().get(0).getName()).isEqualTo("c1");
    assertThat(actual.getColumns().get(0).getTextValue()).isEqualTo("v1");
    assertThat(actual.getColumns().get(1)).isInstanceOf(IntColumn.class);
    assertThat(actual.getColumns().get(1).getName()).isEqualTo("c2");
    assertThat(actual.getColumns().get(1).getIntValue()).isEqualTo(2);
  }

  @Test
  public void getClusteringKey_ShouldReturnClusteringKey() {
    // Arrange
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.INT)
            .addColumn("c3", DataType.INT)
            .addColumn("c4", DataType.BIGINT)
            .addColumn("c5", DataType.DOUBLE)
            .addPartitionKey("c1")
            .addPartitionKey("c2")
            .addClusteringKey("c3")
            .addClusteringKey("c4")
            .build();

    Result result =
        new ResultImpl(
            ImmutableMap.of(
                "c1", TextColumn.of("c1", "v1"),
                "c2", IntColumn.of("c2", 2),
                "c3", IntColumn.of("c3", 3),
                "c4", BigIntColumn.of("c4", 4L),
                "c5", DoubleColumn.of("c5", 5.0)),
            tableMetadata);

    // Act
    Optional<Key> actual = ScalarDbUtils.getClusteringKey(result, tableMetadata);

    // Assert
    assertThat(actual).isPresent();
    assertThat(actual.get().getColumns().size()).isEqualTo(2);
    assertThat(actual.get().getColumns().get(0)).isInstanceOf(IntColumn.class);
    assertThat(actual.get().getColumns().get(0).getName()).isEqualTo("c3");
    assertThat(actual.get().getColumns().get(0).getIntValue()).isEqualTo(3);
    assertThat(actual.get().getColumns().get(1)).isInstanceOf(BigIntColumn.class);
    assertThat(actual.get().getColumns().get(1).getName()).isEqualTo("c4");
    assertThat(actual.get().getColumns().get(1).getBigIntValue()).isEqualTo(4L);
  }

  @Test
  public void getClusteringKey_TableMetadataWithoutClusteringKey_ShouldReturnClusteringKey() {
    // Arrange
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.INT)
            .addColumn("c3", DataType.INT)
            .addColumn("c4", DataType.BIGINT)
            .addColumn("c5", DataType.DOUBLE)
            .addPartitionKey("c1")
            .addPartitionKey("c2")
            .build();

    Result result =
        new ResultImpl(
            ImmutableMap.of(
                "c1", TextColumn.of("c1", "v1"),
                "c2", IntColumn.of("c2", 2),
                "c3", IntColumn.of("c3", 3),
                "c4", BigIntColumn.of("c4", 4L),
                "c5", DoubleColumn.of("c5", 5.0)),
            tableMetadata);

    // Act
    Optional<Key> actual = ScalarDbUtils.getClusteringKey(result, tableMetadata);

    // Assert
    assertThat(actual).isNotPresent();
  }
}
