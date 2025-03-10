package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ScanBuilderTest {
  private static final String NAMESPACE_1 = "namespace1";
  private static final String NAMESPACE_2 = "namespace2";
  private static final String TABLE_1 = "table1";
  private static final String TABLE_2 = "table2";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";

  @Mock private Key partitionKey1;
  @Mock private Key partitionKey2;
  @Mock private Key startClusteringKey1;
  @Mock private Key startClusteringKey2;
  @Mock private Key endClusteringKey1;
  @Mock private Key endClusteringKey2;
  @Mock private Scan.Ordering ordering1;
  @Mock private Scan.Ordering ordering2;
  @Mock private Scan.Ordering ordering3;
  @Mock private Scan.Ordering ordering4;
  @Mock private Scan.Ordering ordering5;
  @Mock private Key indexKey1;
  @Mock private Key indexKey2;
  @Mock private ConditionalExpression condition;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private AndConditionSet prepareAndConditionSet() {
    return ConditionSetBuilder.condition(
            ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1))
        .and(ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1))
        .build();
  }

  private AndConditionSet prepareAnotherAndConditionSet() {
    return ConditionSetBuilder.condition(
            ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1))
        .and(ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_2))
        .build();
  }

  private OrConditionSet prepareOrConditionSet() {
    return ConditionSetBuilder.condition(
            ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1))
        .or(ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1))
        .build();
  }

  private OrConditionSet prepareAnotherOrConditionSet() {
    return ConditionSetBuilder.condition(
            ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1))
        .or(ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_2))
        .build();
  }

  @Test
  public void buildScan_WithMandatoryParameters_ShouldBuildScanWithMandatoryParameters() {
    // Arrange Act
    Scan actual = Scan.newBuilder().table(TABLE_1).partitionKey(partitionKey1).build();

    // Assert
    assertThat(actual).isEqualTo(new Scan(partitionKey1).forTable(TABLE_1));
  }

  @Test
  public void buildScan_ScanWithAllParameters_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1)
            .end(endClusteringKey1, true)
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .readTag("policyName1", "readTag")
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new Scan(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(
                    "a1",
                    "v1",
                    "a2",
                    "v2",
                    "a3",
                    "v3",
                    AbacOperationAttributes.READ_TAG_PREFIX + "policyName1",
                    "readTag"),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(),
                startClusteringKey1,
                true,
                endClusteringKey1,
                true,
                Arrays.asList(ordering1, ordering2, ordering3, ordering4, ordering5),
                10));
  }

  @Test
  public void buildScan_ScanWithInclusiveStartAndEnd_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan1 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1)
            .end(endClusteringKey1)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1, true)
            .end(endClusteringKey1, true)
            .build();

    // Assert
    Scan expectedScan =
        new Scan(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withStart(startClusteringKey1, true)
            .withEnd(endClusteringKey1, true);
    assertThat(scan1).isEqualTo(expectedScan);
    assertThat(scan2).isEqualTo(expectedScan);
  }

  @Test
  public void buildScan_ScanWithExclusiveStartAndEnd_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1, false)
            .end(endClusteringKey1, false)
            .build();

    // Assert
    Scan expectedScan =
        new Scan(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withStart(startClusteringKey1, false)
            .withEnd(endClusteringKey1, false);
    assertThat(scan).isEqualTo(expectedScan);
  }

  @Test
  public void buildScan_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Scan existingScan =
        new Scan(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withStart(startClusteringKey1)
            .withEnd(endClusteringKey1)
            .withOrdering(ordering1)
            .withOrdering(ordering2)
            .withLimit(10)
            .withProjections(Arrays.asList("pk1", "ck1"));

    // Act
    Scan newScan = Scan.newBuilder(existingScan).build();

    // Assert
    assertThat(newScan).isEqualTo(existingScan);
  }

  @Test
  public void buildScan_FromExistingAndUpdateAllParameters_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan existingScan1 =
        new Scan(
            NAMESPACE_1,
            TABLE_1,
            partitionKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"),
            Arrays.asList("pk1", "ck1", "ck2"),
            ImmutableSet.of(),
            startClusteringKey1,
            true,
            endClusteringKey1,
            true,
            Arrays.asList(ordering1, ordering2),
            10);
    Scan existingScan2 =
        new Scan(
            NAMESPACE_1,
            TABLE_1,
            partitionKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of(AbacOperationAttributes.READ_TAG_PREFIX + "policyName1", "readTag"),
            Collections.emptyList(),
            ImmutableSet.of(),
            startClusteringKey1,
            false,
            endClusteringKey1,
            false,
            Arrays.asList(ordering1, ordering2),
            10);

    // Act
    Scan newScan1 =
        Scan.newBuilder(existingScan1)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .start(startClusteringKey2, false)
            .end(endClusteringKey2, false)
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("pk2", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .readTag("policyName1", "readTag")
            .build();
    Scan newScan2 = Scan.newBuilder(existingScan2).clearReadTag("policyName1").build();

    // Assert
    assertThat(newScan1)
        .isEqualTo(
            new Scan(
                NAMESPACE_2,
                TABLE_2,
                partitionKey2,
                Consistency.LINEARIZABLE,
                ImmutableMap.of(
                    "a4",
                    "v4",
                    "a5",
                    "v5",
                    "a6",
                    "v6",
                    AbacOperationAttributes.READ_TAG_PREFIX + "policyName1",
                    "readTag"),
                Arrays.asList("pk2", "ck2", "ck3", "ck4", "ck5"),
                ImmutableSet.of(),
                startClusteringKey2,
                false,
                endClusteringKey2,
                false,
                Arrays.asList(ordering3, ordering4, ordering5, ordering1, ordering2),
                5));
    assertThat(newScan2)
        .isEqualTo(
            new Scan(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Collections.emptyList(),
                ImmutableSet.of(),
                startClusteringKey1,
                false,
                endClusteringKey1,
                false,
                Arrays.asList(ordering1, ordering2),
                10));
  }

  @Test
  public void buildScan_FromExistingAndClearBoundaries_ShouldBuildScanWithoutBoundaries() {
    // Arrange
    Scan existingScan =
        new Scan(partitionKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withStart(startClusteringKey1)
            .withEnd(endClusteringKey1);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).clearStart().clearEnd().build();

    // Assert
    assertThat(newScan)
        .isEqualTo(new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void buildScan_FromExistingAndClearNamespace_ShouldBuildScanWithoutNamespace() {
    // Arrange
    Scan existingScan = new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).clearNamespace().build();

    // Assert
    assertThat(newScan).isEqualTo(new Scan(partitionKey1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScan_FromExistingWithUnsupportedOperation_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Scan existingScan = new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).indexKey(indexKey1))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void buildScanAll_WithMandatoryParameters_ShouldBuildScanWithMandatoryParameters() {
    // Arrange Act
    Scan actual = Scan.newBuilder().table(TABLE_1).all().build();

    // Assert
    assertThat(actual).isEqualTo(new ScanAll().forTable(TABLE_1));
  }

  @Test
  public void buildScanAll_ScanWithAllParameters_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .readTag("policyName1", "readTag")
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                Consistency.EVENTUAL,
                ImmutableMap.of(
                    "a1",
                    "v1",
                    "a2",
                    "v2",
                    "a3",
                    "v3",
                    AbacOperationAttributes.READ_TAG_PREFIX + "policyName1",
                    "readTag"),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10))),
                Arrays.asList(ordering1, ordering2, ordering3, ordering4, ordering5),
                10));
  }

  @Test
  public void buildScanAll_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Scan existingScan =
        new ScanAll()
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withLimit(10)
            .withProjections(Arrays.asList("pk1", "ck1"));

    // Act
    Scan newScan = Scan.newBuilder(existingScan).build();

    // Assert
    assertThat(newScan).isEqualTo(existingScan);
  }

  @Test
  public void
      buildScanAll_FromExistingAndUpdateAllParameters_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan existingScan1 =
        new ScanAll(
            NAMESPACE_1,
            TABLE_1,
            Consistency.EVENTUAL,
            ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"),
            Arrays.asList("pk1", "ck1"),
            ImmutableSet.of(),
            ImmutableList.of(ordering1, ordering2),
            10);
    Scan existingScan2 =
        new ScanAll(
            NAMESPACE_1,
            TABLE_1,
            Consistency.EVENTUAL,
            ImmutableMap.of(AbacOperationAttributes.READ_TAG_PREFIX + "policyName1", "readTag"),
            Collections.emptyList(),
            ImmutableSet.of(),
            ImmutableList.of(ordering1, ordering2),
            10);

    // Act
    Scan newScan1 =
        Scan.newBuilder(existingScan1)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("pk2", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .readTag("policyName1", "readTag")
            .build();
    Scan newScan2 = Scan.newBuilder(existingScan2).clearReadTag("policyName1").build();

    // Assert
    assertThat(newScan1)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_2,
                TABLE_2,
                Consistency.LINEARIZABLE,
                ImmutableMap.of(
                    "a4",
                    "v4",
                    "a5",
                    "v5",
                    "a6",
                    "v6",
                    AbacOperationAttributes.READ_TAG_PREFIX + "policyName1",
                    "readTag"),
                Arrays.asList("pk2", "ck2", "ck3", "ck4", "ck5"),
                ImmutableSet.of(),
                ImmutableList.of(ordering3, ordering4, ordering5, ordering1, ordering2),
                5));
    assertThat(newScan2)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Collections.emptyList(),
                ImmutableSet.of(),
                ImmutableList.of(ordering1, ordering2),
                10));
  }

  @Test
  public void
      buildScanAll_FromExistingWithUnsupportedOperation_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Scan existingScan = new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).partitionKey(partitionKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).indexKey(indexKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).start(startClusteringKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).start(startClusteringKey1, false))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).end(endClusteringKey1, false))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).end(endClusteringKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearStart())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearEnd())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void buildScanAll_FromExistingAndClearNamespace_ShouldBuildScanWithoutNamespace() {
    // Arrange
    ScanAll existingScan = new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).clearNamespace().build();

    // Assert
    assertThat(newScan).isEqualTo(new ScanAll().forTable(TABLE_1));
  }

  @Test
  public void buildScanWithIndex_WithMandatoryParameters_ShouldBuildScanWithMandatoryParameters() {
    // Arrange Act
    Scan actual = Scan.newBuilder().table(TABLE_1).indexKey(indexKey1).build();

    // Assert
    assertThat(actual).isEqualTo(new ScanWithIndex(indexKey1).forTable(TABLE_1));
  }

  @Test
  public void buildScanWithIndex_ScanWithAllParameters_ShouldBuildScanCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .readTag("policyName1", "readTag")
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanWithIndex(
                NAMESPACE_1,
                TABLE_1,
                indexKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(
                    "a1",
                    "v1",
                    "a2",
                    "v2",
                    "a3",
                    "v3",
                    AbacOperationAttributes.READ_TAG_PREFIX + "policyName1",
                    "readTag"),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(),
                10));
  }

  @Test
  public void buildScanWithIndex_FromExistingWithoutChange_ShouldCopy() {
    // Arrange
    Scan existingScan =
        new ScanWithIndex(indexKey1)
            .forNamespace(NAMESPACE_1)
            .forTable(TABLE_1)
            .withConsistency(Consistency.EVENTUAL)
            .withLimit(10)
            .withProjections(Arrays.asList("pk1", "ck1"));

    // Act
    Scan newScan = Scan.newBuilder(existingScan).build();

    // Assert
    assertThat(newScan).isEqualTo(existingScan);
  }

  @Test
  public void
      buildScanWithIndex_FromExistingAndUpdateAllParameters_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan existingScan1 =
        new ScanWithIndex(
            NAMESPACE_1,
            TABLE_1,
            indexKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of("a1", "v1", "a2", "v2", "a3", "v3"),
            Arrays.asList("pk1", "ck1"),
            ImmutableSet.of(),
            10);
    Scan existingScan2 =
        new ScanWithIndex(
            NAMESPACE_1,
            TABLE_1,
            indexKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of(AbacOperationAttributes.READ_TAG_PREFIX + "policyName1", "readTag"),
            Collections.emptyList(),
            ImmutableSet.of(),
            10);

    // Act
    Scan newScan1 =
        Scan.newBuilder(existingScan1)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .indexKey(indexKey2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("pk2", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .readTag("policyName1", "readTag")
            .build();
    Scan newScan2 = Scan.newBuilder(existingScan2).clearReadTag("policyName1").build();

    // Assert
    assertThat(newScan1)
        .isEqualTo(
            new ScanWithIndex(
                NAMESPACE_2,
                TABLE_2,
                indexKey2,
                Consistency.LINEARIZABLE,
                ImmutableMap.of(
                    "a4",
                    "v4",
                    "a5",
                    "v5",
                    "a6",
                    "v6",
                    AbacOperationAttributes.READ_TAG_PREFIX + "policyName1",
                    "readTag"),
                Arrays.asList("pk2", "ck2", "ck3", "ck4", "ck5"),
                ImmutableSet.of(),
                5));
    assertThat(newScan2)
        .isEqualTo(
            new ScanWithIndex(
                NAMESPACE_1,
                TABLE_1,
                indexKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Collections.emptyList(),
                ImmutableSet.of(),
                10));
  }

  @Test
  public void
      buildScanWithIndex_FromExistingWithUnsupportedOperation_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Scan existingScan = new ScanWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act Assert
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).partitionKey(partitionKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearOrderings())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).start(startClusteringKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).start(startClusteringKey1, false))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).end(endClusteringKey1, false))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).end(endClusteringKey1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).ordering(ordering1))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearStart())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).clearEnd())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void buildScanWithIndex_FromExistingAndClearNamespace_ShouldBuildScanWithoutNamespace() {
    // Arrange
    ScanWithIndex existingScan =
        new ScanWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1);

    // Act
    Scan newScan = Scan.newBuilder(existingScan).clearNamespace().build();

    // Assert
    assertThat(newScan).isEqualTo(new ScanWithIndex(indexKey1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithConjunctiveNormalForm_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan1 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .limit(10)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan4 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan5 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .build();

    // Assert
    Scan expected =
        new Scan(
            NAMESPACE_1,
            TABLE_1,
            partitionKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of(),
            Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
            ImmutableSet.of(
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck3").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10)),
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck4").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10))),
            null,
            false,
            null,
            false,
            Arrays.asList(ordering1, ordering2, ordering3, ordering4, ordering5),
            10);

    assertThat(scan1).isEqualTo(expected);
    assertThat(scan2).isEqualTo(expected);
    assertThat(scan3).isEqualTo(expected);
    assertThat(scan4).isEqualTo(expected);
    assertThat(scan5).isEqualTo(expected);
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithConditionAndConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new Scan(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))),
                null,
                false,
                null,
                false,
                ImmutableList.of(),
                10));
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithDisjunctiveNormalForm_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan1 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .limit(10)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan4 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan5 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .build();

    // Assert
    Scan expected =
        new Scan(
            NAMESPACE_1,
            TABLE_1,
            partitionKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of(),
            Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
            ImmutableSet.of(
                Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                Conjunction.of(ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                Conjunction.of(
                    ConditionBuilder.column("ck3").isGreaterThanInt(10),
                    ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))),
            null,
            false,
            null,
            false,
            Arrays.asList(ordering1, ordering2, ordering3, ordering4, ordering5),
            10);

    assertThat(scan1).isEqualTo(expected);
    assertThat(scan2).isEqualTo(expected);
    assertThat(scan3).isEqualTo(expected);
    assertThat(scan4).isEqualTo(expected);
    assertThat(scan5).isEqualTo(expected);
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithConditionOrConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new Scan(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))),
                null,
                false,
                null,
                false,
                ImmutableList.of(),
                10));
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithTwoOrConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new Scan(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("col1").isGreaterThanInt(10),
                        ConditionBuilder.column("col2").isGreaterThanInt(10))),
                null,
                false,
                null,
                false,
                ImmutableList.of(),
                10));
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithOrConditionSets_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .whereAnd(
                ImmutableSet.of(
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new Scan(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10))),
                null,
                false,
                null,
                false,
                ImmutableList.of(),
                10));
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithAndConditionSets_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .whereOr(
                ImmutableSet.of(
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new Scan(
                NAMESPACE_1,
                TABLE_1,
                partitionKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("ck3").isGreaterThanInt(10))),
                null,
                false,
                null,
                false,
                ImmutableList.of(),
                10));
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithEmptyOrConditionSet_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .and(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(scan).isEqualTo(new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithEmptyAndConditionSet_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .where(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .or(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(scan).isEqualTo(new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithEmptyOrConditionSets_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .whereAnd(ImmutableSet.of())
            .build();

    // Assert
    assertThat(scan).isEqualTo(new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScan_ScanByPartitionKeyWithEmptyAndConditionSets_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .whereOr(ImmutableSet.of())
            .build();

    // Assert
    assertThat(scan).isEqualTo(new Scan(partitionKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScanWithIndex_WithConjunctiveNormalForm_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan1 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .limit(10)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan4 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .build();

    // Assert
    Scan expected =
        new ScanWithIndex(
            NAMESPACE_1,
            TABLE_1,
            indexKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of(),
            Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
            ImmutableSet.of(
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck3").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10)),
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck4").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10))),
            10);

    assertThat(scan1).isEqualTo(expected);
    assertThat(scan2).isEqualTo(expected);
    assertThat(scan3).isEqualTo(expected);
    assertThat(scan4).isEqualTo(expected);
  }

  @Test
  public void
      buildScanWithIndex_WithConditionAndConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanWithIndex(
                NAMESPACE_1,
                TABLE_1,
                indexKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))),
                10));
  }

  @Test
  public void
      buildScanWithIndex_WithDisjunctiveNormalForm_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan1 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .limit(10)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan4 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .build();

    // Assert
    Scan expected =
        new ScanWithIndex(
            NAMESPACE_1,
            TABLE_1,
            indexKey1,
            Consistency.EVENTUAL,
            ImmutableMap.of(),
            Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
            ImmutableSet.of(
                Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                Conjunction.of(ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                Conjunction.of(
                    ConditionBuilder.column("ck3").isGreaterThanInt(10),
                    ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))),
            10);

    assertThat(scan1).isEqualTo(expected);
    assertThat(scan2).isEqualTo(expected);
    assertThat(scan3).isEqualTo(expected);
    assertThat(scan4).isEqualTo(expected);
  }

  @Test
  public void
      buildScanWithIndex_WithConditionOrConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanWithIndex(
                NAMESPACE_1,
                TABLE_1,
                indexKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))),
                10));
  }

  @Test
  public void buildScanWithIndex_WithTwoOrConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanWithIndex(
                NAMESPACE_1,
                TABLE_1,
                indexKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("col1").isGreaterThanInt(10),
                        ConditionBuilder.column("col2").isGreaterThanInt(10))),
                10));
  }

  @Test
  public void buildScanWithIndex_WithOrConditionSets_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .whereAnd(
                ImmutableSet.of(
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanWithIndex(
                NAMESPACE_1,
                TABLE_1,
                indexKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10))),
                10));
  }

  @Test
  public void buildScanWithIndex_WithAndConditionSets_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .whereOr(
                ImmutableSet.of(
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanWithIndex(
                NAMESPACE_1,
                TABLE_1,
                indexKey1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("ck3").isGreaterThanInt(10))),
                10));
  }

  @Test
  public void
      buildScanWithIndex_WithEmptyOrConditionSet_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .and(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(new ScanWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScanWithIndex_WithEmptyAndConditionSet_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .or(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(new ScanWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScanWithIndex_WithEmptyOrConditionSets_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .whereAnd(ImmutableSet.of())
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(new ScanWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScanWithIndex_WithEmptyAndConditionSets_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .whereOr(ImmutableSet.of())
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(new ScanWithIndex(indexKey1).forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScan_FromExistingWithConditionsAndUpdateAllParameters_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1)
            .end(endClusteringKey1, false)
            .where(ConditionBuilder.column("pk1").isGreaterThanInt(10))
            .ordering(ordering1)
            .ordering(ordering2)
            .limit(10)
            .projection("pk1")
            .consistency(Consistency.EVENTUAL)
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();
    Scan expected =
        new Scan(
            NAMESPACE_2,
            TABLE_2,
            partitionKey2,
            Consistency.LINEARIZABLE,
            ImmutableMap.of("a4", "v4", "a5", "v5", "a6", "v6"),
            Arrays.asList("ck1", "ck2", "ck3", "ck4", "ck5"),
            ImmutableSet.of(
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck3").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10)),
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck4").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10))),
            startClusteringKey2,
            false,
            endClusteringKey2,
            true,
            Arrays.asList(ordering3, ordering4, ordering5, ordering1, ordering2),
            5);

    // Act
    Scan newScan1 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .partitionKey(partitionKey2)
            .start(startClusteringKey2, false)
            .end(endClusteringKey2)
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();
    Scan newScan2 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .start(startClusteringKey2, false)
            .end(endClusteringKey2)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();
    Scan newScan3 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .start(startClusteringKey2, false)
            .end(endClusteringKey2)
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();
    Scan newScan4 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .start(startClusteringKey2, false)
            .end(endClusteringKey2)
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();
    Scan newScan5 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .start(startClusteringKey2, false)
            .end(endClusteringKey2)
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();
    Scan newScan6 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .start(startClusteringKey2, false)
            .end(endClusteringKey2)
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();

    // Assert
    assertThat(newScan1).isEqualTo(expected);
    assertThat(newScan2).isEqualTo(expected);
    assertThat(newScan3).isEqualTo(expected);
    assertThat(newScan4).isEqualTo(expected);
    assertThat(newScan5).isEqualTo(expected);
    assertThat(newScan6).isEqualTo(expected);
  }

  @Test
  public void
      buildScan_FromExistingWithConditionsAndDifferentClusteringKeyPatterns_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .partitionKey(partitionKey1)
            .start(startClusteringKey1, false)
            .end(endClusteringKey1)
            .where(ConditionBuilder.column("pk1").isGreaterThanInt(10))
            .ordering(ordering1)
            .ordering(ordering2)
            .limit(10)
            .projection("pk1")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan expected =
        new Scan(
            NAMESPACE_2,
            TABLE_2,
            partitionKey2,
            Consistency.LINEARIZABLE,
            ImmutableMap.of(),
            Arrays.asList("ck1", "ck2", "ck3", "ck4", "ck5"),
            ImmutableSet.of(
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck3").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10)),
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck4").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10))),
            startClusteringKey2,
            true,
            endClusteringKey2,
            false,
            Arrays.asList(ordering3, ordering4, ordering5, ordering1, ordering2),
            5);

    // Act
    Scan newScan1 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .partitionKey(partitionKey2)
            .start(startClusteringKey2)
            .end(endClusteringKey2, false)
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .build();
    Scan newScan2 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .partitionKey(partitionKey2)
            .start(startClusteringKey2)
            .end(endClusteringKey2, false)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .build();

    // Assert
    assertThat(newScan1).isEqualTo(expected);
    assertThat(newScan2).isEqualTo(expected);
  }

  @Test
  public void
      buildScanWithIndex_FromExistingWithConditionsAndUpdateAllParameters_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .indexKey(indexKey1)
            .where(ConditionBuilder.column("pk1").isGreaterThanInt(10))
            .limit(10)
            .projection("pk1")
            .consistency(Consistency.EVENTUAL)
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();
    Scan expected =
        new ScanWithIndex(
            NAMESPACE_2,
            TABLE_2,
            indexKey2,
            Consistency.LINEARIZABLE,
            ImmutableMap.of("a4", "v4", "a5", "v5", "a6", "v6"),
            Arrays.asList("ck1", "ck2", "ck3", "ck4", "ck5"),
            ImmutableSet.of(
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck3").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10)),
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck4").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10))),
            5);

    // Act
    Scan newScan1 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .indexKey(indexKey2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();
    Scan newScan2 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .indexKey(indexKey2)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();
    Scan newScan3 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .indexKey(indexKey2)
            .limit(5)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();
    Scan newScan4 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .indexKey(indexKey2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();
    Scan newScan5 =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .indexKey(indexKey2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();

    // Assert
    assertThat(newScan1).isEqualTo(expected);
    assertThat(newScan2).isEqualTo(expected);
    assertThat(newScan3).isEqualTo(expected);
    assertThat(newScan4).isEqualTo(expected);
    assertThat(newScan5).isEqualTo(expected);
  }

  @Test
  public void buildScanAll_ScanWithConjunctiveNormalForm_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan1 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .limit(10)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan4 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan5 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .build();

    // Assert
    ScanAll expected =
        new ScanAll(
            NAMESPACE_1,
            TABLE_1,
            Consistency.EVENTUAL,
            ImmutableMap.of(),
            Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
            ImmutableSet.of(
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck3").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10)),
                Conjunction.of(
                    ConditionBuilder.column("ck1").isGreaterThanInt(10),
                    ConditionBuilder.column("ck2").isGreaterThanInt(10),
                    ConditionBuilder.column("ck4").isGreaterThanInt(10),
                    ConditionBuilder.column("col1").isGreaterThanInt(10))),
            Arrays.asList(ordering1, ordering2, ordering3, ordering4, ordering5),
            10);

    assertThat(scan1).isEqualTo(expected);
    assertThat(scan2).isEqualTo(expected);
    assertThat(scan3).isEqualTo(expected);
    assertThat(scan4).isEqualTo(expected);
    assertThat(scan5).isEqualTo(expected);
  }

  @Test
  public void
      buildScanAll_ScanWithConditionAndConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))),
                ImmutableList.of(),
                10));
  }

  @Test
  public void buildScanAll_ScanWithTwoAndConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col2").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col2").isGreaterThanInt(10))),
                ImmutableList.of(),
                10));
  }

  @Test
  public void buildScanAll_ScanWithDisjunctiveNormalForm_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan1 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .limit(10)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan4 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();
    Scan scan5 =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .consistency(Consistency.EVENTUAL)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .ordering(ordering1)
            .orderings(Arrays.asList(ordering2, ordering3))
            .orderings(ordering4, ordering5)
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .build();

    // Assert
    ScanAll expected =
        new ScanAll(
            NAMESPACE_1,
            TABLE_1,
            Consistency.EVENTUAL,
            ImmutableMap.of(),
            Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
            ImmutableSet.of(
                Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                Conjunction.of(ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                Conjunction.of(
                    ConditionBuilder.column("ck3").isGreaterThanInt(10),
                    ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))),
            Arrays.asList(ordering1, ordering2, ordering3, ordering4, ordering5),
            10);

    assertThat(scan1).isEqualTo(expected);
    assertThat(scan2).isEqualTo(expected);
    assertThat(scan3).isEqualTo(expected);
    assertThat(scan4).isEqualTo(expected);
    assertThat(scan5).isEqualTo(expected);
  }

  @Test
  public void
      buildScanAll_ScanWithConditionOrConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))),
                ImmutableList.of(),
                10));
  }

  @Test
  public void buildScanAll_ScanWithTwoOrConditionSet_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("col1").isGreaterThanInt(10),
                        ConditionBuilder.column("col2").isGreaterThanInt(10))),
                ImmutableList.of(),
                10));
  }

  @Test
  public void buildScanAll_ScanWithOrConditionSets_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .whereAnd(
                ImmutableSet.of(
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10))),
                ImmutableList.of(),
                10));
  }

  @Test
  public void buildScanAll_ScanWithAndConditionSets_ShouldBuildScanWithConditionsCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .whereOr(
                ImmutableSet.of(
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .limit(10)
            .projections(Arrays.asList("pk1", "ck1"))
            .projection("ck2")
            .projections("ck3", "ck4")
            .consistency(Consistency.EVENTUAL)
            .build();

    // Assert
    assertThat(scan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                Consistency.EVENTUAL,
                ImmutableMap.of(),
                Arrays.asList("pk1", "ck1", "ck2", "ck3", "ck4"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("ck3").isGreaterThanInt(10))),
                ImmutableList.of(),
                10));
  }

  @Test
  public void
      buildScanAll_ScanWithEmptyOrConditionSet_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .and(ConditionSetBuilder.orConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(scan).isEqualTo(new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScanAll_ScanWithEmptyAndConditionSet_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .or(ConditionSetBuilder.andConditionSet(ImmutableSet.of()).build())
            .build();

    // Assert
    assertThat(scan).isEqualTo(new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScanAll_ScanWithEmptyOrConditionSets_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .whereAnd(ImmutableSet.of())
            .build();

    // Assert
    assertThat(scan).isEqualTo(new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScanAll_ScanWithEmptyAndConditionSets_ShouldBuildScanWithoutConjunctionCorrectly() {
    // Arrange Act
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .whereOr(ImmutableSet.of())
            .build();

    // Assert
    assertThat(scan).isEqualTo(new ScanAll().forNamespace(NAMESPACE_1).forTable(TABLE_1));
  }

  @Test
  public void
      buildScanAll_FromExistingWithConditionsAndUpdateAllParameters_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(ConditionBuilder.column("pk1").isGreaterThanInt(10))
            .ordering(ordering1)
            .ordering(ordering2)
            .limit(10)
            .projection("pk1")
            .consistency(Consistency.EVENTUAL)
            .attribute("a1", "v1")
            .attributes(ImmutableMap.of("a2", "v2", "a3", "v3"))
            .build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .limit(5)
            .clearProjections()
            .projections(Arrays.asList("ck1", "ck2"))
            .projection("ck3")
            .projections("ck4", "ck5")
            .consistency(Consistency.LINEARIZABLE)
            .clearAttributes()
            .attribute("a4", "v4")
            .attributes(ImmutableMap.of("a5", "v5", "a6", "v6", "a7", "v7"))
            .clearAttribute("a7")
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                Consistency.LINEARIZABLE,
                ImmutableMap.of("a4", "v4", "a5", "v5", "a6", "v6"),
                Arrays.asList("ck1", "ck2", "ck3", "ck4", "ck5"),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))),
                Arrays.asList(ordering3, ordering4, ordering5, ordering1, ordering2),
                5));
  }

  @Test
  public void
      buildScanAll_FromExistingAndAddConditionAndConditionSet_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10))),
                ImmutableList.of(),
                0));
  }

  @Test
  public void
      buildScanAll_FromExistingAndAddTwoAndConditionSet_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .and(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .or(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("col2").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck4").isGreaterThanInt(10),
                        ConditionBuilder.column("col2").isGreaterThanInt(10))),
                ImmutableList.of(),
                0));
  }

  @Test
  public void
      buildScanAll_FromExistingAndAddTwoOrConditions_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(ConditionBuilder.column("ck2").isGreaterThanInt(10))
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("ck2").isGreaterThanInt(10))),
                ImmutableList.of(),
                0));
  }

  @Test
  public void
      buildScanAll_FromExistingAndAddConditionOrConditionSet_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(ConditionBuilder.column("col1").isGreaterThanInt(10))
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("col1").isGreaterThanInt(10))),
                ImmutableList.of(),
                0));
  }

  @Test
  public void
      buildScanAll_FromExistingAndAddTwoOrConditionSet_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .where(
                ConditionSetBuilder.condition(ConditionBuilder.column("ck3").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("ck4").isGreaterThanInt(10))
                    .build())
            .or(
                ConditionSetBuilder.condition(ConditionBuilder.column("col1").isGreaterThanInt(10))
                    .and(ConditionBuilder.column("col2").isGreaterThanInt(10))
                    .build())
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck3").isGreaterThanInt(10),
                        ConditionBuilder.column("ck4").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("col1").isGreaterThanInt(10),
                        ConditionBuilder.column("col2").isGreaterThanInt(10))),
                ImmutableList.of(),
                0));
  }

  @Test
  public void
      buildScanAll_FromExistingWithConditionsAndAddOrderings_ShouldBuildScanWithConditionsAndUpdatedParameters() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .clearOrderings()
            .ordering(ordering3)
            .orderings(Arrays.asList(ordering4, ordering5))
            .orderings(ordering1, ordering2)
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10))),
                Arrays.asList(ordering3, ordering4, ordering5, ordering1, ordering2),
                0));
  }

  @Test
  public void buildScanAll_FromExistingWithOrConditionSets_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .whereAnd(
                ImmutableSet.of(
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.orConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10)),
                    Conjunction.of(
                        ConditionBuilder.column("ck2").isGreaterThanInt(10),
                        ConditionBuilder.column("ck3").isGreaterThanInt(10))),
                ImmutableList.of(),
                0));
  }

  @Test
  public void buildScanAll_FromExistingWithAndConditionSets_ShouldBuildScanWithUpdatedParameters() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .whereOr(
                ImmutableSet.of(
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(
                                ConditionBuilder.column("ck1").isGreaterThanInt(10),
                                ConditionBuilder.column("ck2").isGreaterThanInt(10)))
                        .build(),
                    ConditionSetBuilder.andConditionSet(
                            ImmutableSet.of(ConditionBuilder.column("ck3").isGreaterThanInt(10)))
                        .build()))
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_1,
                TABLE_1,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("ck1").isGreaterThanInt(10),
                        ConditionBuilder.column("ck2").isGreaterThanInt(10)),
                    Conjunction.of(ConditionBuilder.column("ck3").isGreaterThanInt(10))),
                ImmutableList.of(),
                0));
  }

  @Test
  public void
      buildScanAll_FromExistingWithConditionAndCallWhereBeforeClearingCondition_ShouldThrowUnsupportedOperationException() {
    // Arrange
    Scan existingScan =
        Scan.newBuilder()
            .namespace(NAMESPACE_1)
            .table(TABLE_1)
            .all()
            .where(ConditionBuilder.column("pk1").isGreaterThanInt(10))
            .build();

    // Act Assert
    assertThatThrownBy(() -> Scan.newBuilder(existingScan).where(condition))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void
      buildScanAll_FromExistingAndUpdateNamespaceAndTableAfterWhere_ShouldBuildScanWithNewNamespaceAndTable() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .namespace(NAMESPACE_2)
            .table(TABLE_2)
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                NAMESPACE_2,
                TABLE_2,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10))),
                ImmutableList.of(),
                0));
  }

  @Test
  public void
      buildScanAll_FromExistingAndClearNamespaceAfterWhere_ShouldBuildScanWithoutNamespace() {
    // Arrange
    Scan scan = Scan.newBuilder().namespace(NAMESPACE_1).table(TABLE_1).all().build();

    // Act
    Scan newScan =
        Scan.newBuilder(scan)
            .clearConditions()
            .where(ConditionBuilder.column("ck1").isGreaterThanInt(10))
            .clearNamespace()
            .build();

    // Assert
    assertThat(newScan)
        .isEqualTo(
            new ScanAll(
                null,
                TABLE_1,
                null,
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("ck1").isGreaterThanInt(10))),
                ImmutableList.of(),
                0));
  }

  @Test
  public void equals_SameAndConditionSetInstanceGiven_ShouldReturnTrue() {
    // Arrange
    AndConditionSet andConditionSet = prepareAndConditionSet();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = andConditionSet.equals(andConditionSet);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SameAndConditionSetGiven_ShouldReturnTrue() {
    // Arrange
    AndConditionSet andConditionSet = prepareAndConditionSet();
    AndConditionSet another = prepareAndConditionSet();

    // Act
    boolean ret = andConditionSet.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(andConditionSet.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_AndConditionSetWithDifferentConditionGiven_ShouldReturnFalse() {
    // Arrange
    AndConditionSet andConditionSet = prepareAndConditionSet();
    AndConditionSet another = prepareAnotherAndConditionSet();

    // Act
    boolean ret = andConditionSet.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void equals_SameOrConditionSetInstanceGiven_ShouldReturnTrue() {
    // Arrange
    OrConditionSet orConditionSet = prepareOrConditionSet();

    // Act
    @SuppressWarnings("SelfEquals")
    boolean ret = orConditionSet.equals(orConditionSet);

    // Assert
    assertThat(ret).isTrue();
  }

  @Test
  public void equals_SameOrConditionSetGiven_ShouldReturnTrue() {
    // Arrange
    OrConditionSet orConditionSet = prepareOrConditionSet();
    OrConditionSet another = prepareOrConditionSet();

    // Act
    boolean ret = orConditionSet.equals(another);

    // Assert
    assertThat(ret).isTrue();
    assertThat(orConditionSet.hashCode()).isEqualTo(another.hashCode());
  }

  @Test
  public void equals_OrConditionSetWithDifferentConditionGiven_ShouldReturnFalse() {
    // Arrange
    OrConditionSet orConditionSet = prepareOrConditionSet();
    OrConditionSet another = prepareAnotherOrConditionSet();

    // Act
    boolean ret = orConditionSet.equals(another);

    // Assert
    assertThat(ret).isFalse();
  }

  @Test
  public void
      ConditionSetBuilder_TwoConditionsConnectedWithAndGiven_ShouldBuildAndConditionSetCorrectly() {
    // Arrange Act
    AndConditionSet andConditionSet =
        ConditionSetBuilder.condition(ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1))
            .and(ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1))
            .build();

    // Assert
    assertThat(andConditionSet.getConditions())
        .hasSameElementsAs(
            ImmutableSet.of(
                ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1)));
  }

  @Test
  public void
      ConditionSetBuilder_TwoConditionsConnectedWithOrGiven_ShouldBuildOrConditionSetCorrectly() {
    // Arrange Act
    OrConditionSet orConditionSet =
        ConditionSetBuilder.condition(ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1))
            .or(ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1))
            .build();

    // Assert
    assertThat(orConditionSet.getConditions())
        .hasSameElementsAs(
            ImmutableSet.of(
                ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1)));
  }

  @Test
  public void ConditionSetBuilder_SetOfConditionsGivenForAnd_ShouldBuildAndConditionSetCorrectly() {
    // Arrange Act
    AndConditionSet andConditionSet =
        ConditionSetBuilder.andConditionSet(
                ImmutableSet.of(
                    ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                    ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1)))
            .build();

    // Assert
    assertThat(andConditionSet.getConditions())
        .hasSameElementsAs(
            ImmutableSet.of(
                ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1)));
  }

  @Test
  public void ConditionSetBuilder_SetOfConditionsGivenForOr_ShouldBuildOrConditionSetCorrectly() {
    // Arrange Act
    OrConditionSet orConditionSet =
        ConditionSetBuilder.orConditionSet(
                ImmutableSet.of(
                    ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                    ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1)))
            .build();

    // Assert
    assertThat(orConditionSet.getConditions())
        .hasSameElementsAs(
            ImmutableSet.of(
                ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1)));
  }

  @Test
  public void
      ConditionSetBuilder_AndConditionSetAndConditionConnectedWithAndGiven_ShouldBuildAndConditionSetCorrectly() {
    // Arrange Act
    AndConditionSet andConditionSet =
        ConditionSetBuilder.andConditionSet(
                ImmutableSet.of(
                    ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                    ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1)))
            .and(ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_1))
            .build();

    // Assert
    assertThat(andConditionSet.getConditions())
        .hasSameElementsAs(
            ImmutableSet.of(
                ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1),
                ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_1)));
  }

  @Test
  public void
      ConditionSetBuilder_OrConditionSetAndConditionConnectedWithOrGiven_ShouldBuildOrConditionSetCorrectly() {
    // Arrange Act
    OrConditionSet orConditionSet =
        ConditionSetBuilder.orConditionSet(
                ImmutableSet.of(
                    ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                    ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1)))
            .or(ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_1))
            .build();

    // Assert
    assertThat(orConditionSet.getConditions())
        .hasSameElementsAs(
            ImmutableSet.of(
                ConditionBuilder.column(ANY_NAME_1).isEqualToText(ANY_TEXT_1),
                ConditionBuilder.column(ANY_NAME_2).isEqualToText(ANY_TEXT_1),
                ConditionBuilder.column(ANY_NAME_3).isEqualToText(ANY_TEXT_1)));
  }
}
