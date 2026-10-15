/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestStatsUtilSourceBounds {

  private static final String COLUMN = "col";

  private record Case(
      String name,
      Type type,
      UnaryOperator<PartitionSpec.Builder> transform,
      Object sample,
      boolean exactUpper) {
    @Override
    public String toString() {
      return name;
    }
  }

  private static Case exact(
      String name, Type type, UnaryOperator<PartitionSpec.Builder> transform, Object sample) {
    return new Case(name, type, transform, sample, true);
  }

  private static Case prefix(
      String name, Type type, UnaryOperator<PartitionSpec.Builder> transform, Object sample) {
    return new Case(name, type, transform, sample, false);
  }

  private static Stream<Case> allCases() {
    Type date = Types.DateType.get();
    Type timestamp = Types.TimestampType.withZone();
    Type timestampNano = Types.TimestampNanoType.withZone();
    Type decimal = Types.DecimalType.of(9, 2);

    Stream<Case> identity =
        Stream.of(
            exact("identity int", Types.IntegerType.get(), b -> b.identity(COLUMN), 5),
            exact("identity string", Types.StringType.get(), b -> b.identity(COLUMN), "books"),
            exact("identity date", date, b -> b.identity(COLUMN), 19_000),
            exact("identity timestamp", timestamp, b -> b.identity(COLUMN), 1_700_000_000_123_456L),
            exact("identity decimal", decimal, b -> b.identity(COLUMN), new BigDecimal("12.34")));

    Stream<Case> truncate =
        Stream.of(
            exact("truncate int", Types.IntegerType.get(), b -> b.truncate(COLUMN, 10), 27),
            exact(
                "truncate negative int", Types.IntegerType.get(), b -> b.truncate(COLUMN, 10), -3),
            exact("truncate long", Types.LongType.get(), b -> b.truncate(COLUMN, 100), 12_345L),
            exact(
                "truncate decimal", decimal, b -> b.truncate(COLUMN, 50), new BigDecimal("12.34")),
            exact(
                "truncate negative decimal",
                decimal,
                b -> b.truncate(COLUMN, 50),
                new BigDecimal("-0.01")),
            prefix("truncate string", Types.StringType.get(), b -> b.truncate(COLUMN, 3), "abcdef"),
            exact(
                "truncate short string", Types.StringType.get(), b -> b.truncate(COLUMN, 3), "ab"),
            prefix(
                "truncate binary",
                Types.BinaryType.get(),
                b -> b.truncate(COLUMN, 2),
                ByteBuffer.wrap(new byte[] {1, 2, 3, 4})));

    Stream<Case> dates =
        Stream.of(-1, 0, 19_000)
            .flatMap(
                sample ->
                    Stream.of(
                        exact("year date " + sample, date, b -> b.year(COLUMN), sample),
                        exact("month date " + sample, date, b -> b.month(COLUMN), sample),
                        exact("day date " + sample, date, b -> b.day(COLUMN), sample)));

    Stream<Case> timestamps =
        Stream.of(-86_400_000_000L, -1L, 0L, 1_700_000_000_123_456L)
            .flatMap(
                sample ->
                    Stream.of(
                        exact("year micros " + sample, timestamp, b -> b.year(COLUMN), sample),
                        exact("month micros " + sample, timestamp, b -> b.month(COLUMN), sample),
                        exact("day micros " + sample, timestamp, b -> b.day(COLUMN), sample),
                        exact("hour micros " + sample, timestamp, b -> b.hour(COLUMN), sample),
                        exact(
                            "year nanos " + sample,
                            timestampNano,
                            b -> b.year(COLUMN),
                            sample * 1000),
                        exact(
                            "month nanos " + sample,
                            timestampNano,
                            b -> b.month(COLUMN),
                            sample * 1000),
                        exact(
                            "day nanos " + sample,
                            timestampNano,
                            b -> b.day(COLUMN),
                            sample * 1000),
                        exact(
                            "hour nanos " + sample,
                            timestampNano,
                            b -> b.hour(COLUMN),
                            sample * 1000)));

    return Stream.of(identity, truncate, dates, timestamps).flatMap(s -> s);
  }

  private static Stream<Case> exactCases() {
    return allCases()
        .filter(testCase -> testCase.exactUpper() && !(testCase.sample() instanceof CharSequence));
  }

  @ParameterizedTest
  @MethodSource("allCases")
  void boundsContainSourceValueAndMapBackToPartition(Case testCase) {
    PartitionField field = field(testCase);
    Object partitionValue = apply(field, testCase.type(), testCase.sample());

    StatsUtil.Bounds bounds = StatsUtil.sourceBounds(schema(testCase), field, partitionValue);

    Comparator<Object> comparator = Comparators.forType(testCase.type().asPrimitiveType());
    assertThat(comparator.compare(bounds.lower(), testCase.sample())).isLessThanOrEqualTo(0);
    assertThat(comparator.compare(bounds.upper(), testCase.sample())).isGreaterThanOrEqualTo(0);

    Comparator<Object> partitionComparator =
        Comparators.forType(field.transform().getResultType(testCase.type()).asPrimitiveType());
    assertThat(apply(field, testCase.type(), bounds.lower()))
        .usingComparator(partitionComparator)
        .isEqualTo(partitionValue);
    if (testCase.exactUpper()) {
      assertThat(apply(field, testCase.type(), bounds.upper()))
          .usingComparator(partitionComparator)
          .isEqualTo(partitionValue);
    }
  }

  @ParameterizedTest
  @MethodSource("exactCases")
  void valuesJustOutsideBoundsAreInOtherPartitions(Case testCase) {
    PartitionField field = field(testCase);
    Object partitionValue = apply(field, testCase.type(), testCase.sample());

    StatsUtil.Bounds bounds = StatsUtil.sourceBounds(schema(testCase), field, partitionValue);

    Comparator<Object> partitionComparator =
        Comparators.forType(field.transform().getResultType(testCase.type()).asPrimitiveType());
    assertThat(apply(field, testCase.type(), shift(bounds.lower(), -1)))
        .usingComparator(partitionComparator)
        .isNotEqualTo(partitionValue);
    assertThat(apply(field, testCase.type(), shift(bounds.upper(), 1)))
        .usingComparator(partitionComparator)
        .isNotEqualTo(partitionValue);
  }

  @Test
  void truncatedStringUpperBoundIncrementsPrefix() {
    PartitionField field = field(Types.StringType.get(), b -> b.truncate(COLUMN, 3));

    StatsUtil.Bounds bounds = StatsUtil.sourceBounds(schema(Types.StringType.get()), field, "abc");

    assertThat(bounds.lower()).isEqualTo("abc");
    assertThat(bounds.upper()).isEqualTo("abd");
  }

  @Test
  void truncatedStringShorterThanWidthIsExact() {
    PartitionField field = field(Types.StringType.get(), b -> b.truncate(COLUMN, 3));

    StatsUtil.Bounds bounds = StatsUtil.sourceBounds(schema(Types.StringType.get()), field, "ab");

    assertThat(bounds.lower()).isEqualTo("ab");
    assertThat(bounds.upper()).isEqualTo("ab");
  }

  @Test
  void truncatedStringAtMaxCodePointHasNoUpperBound() {
    PartitionField field = field(Types.StringType.get(), b -> b.truncate(COLUMN, 1));
    String maxCodePoint = new String(Character.toChars(Character.MAX_CODE_POINT));

    StatsUtil.Bounds bounds =
        StatsUtil.sourceBounds(schema(Types.StringType.get()), field, maxCodePoint);

    assertThat(bounds.lower()).isEqualTo(maxCodePoint);
    assertThat(bounds.upper()).isNull();
  }

  @Test
  void truncatedIntUpperBoundSaturates() {
    PartitionField field = field(Types.IntegerType.get(), b -> b.truncate(COLUMN, 10));

    StatsUtil.Bounds bounds =
        StatsUtil.sourceBounds(schema(Types.IntegerType.get()), field, 2_147_483_640);

    assertThat(bounds.lower()).isEqualTo(2_147_483_640);
    assertThat(bounds.upper()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void timestampNanoUpperBoundSaturates() {
    Type type = Types.TimestampNanoType.withZone();
    PartitionField field = field(type, b -> b.year(COLUMN));

    // 2262-01-01 is representable in nanos, but the end of the year is past the maximum
    StatsUtil.Bounds bounds = StatsUtil.sourceBounds(schema(type), field, 292);

    assertThat(bounds.lower()).isEqualTo(9_214_646_400_000_000_000L);
    assertThat(bounds.upper()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void identityBoundsFollowPromotedSourceType() {
    PartitionField field = field(Types.IntegerType.get(), b -> b.identity(COLUMN));

    StatsUtil.Bounds bounds = StatsUtil.sourceBounds(schema(Types.LongType.get()), field, 5);

    assertThat(bounds.lower()).isEqualTo(5L);
    assertThat(bounds.upper()).isEqualTo(5L);
  }

  @Test
  void nullPartitionValueHasNoBounds() {
    PartitionField field = field(Types.TimestampType.withZone(), b -> b.hour(COLUMN));

    assertThat(StatsUtil.sourceBounds(schema(Types.TimestampType.withZone()), field, null))
        .isNull();
  }

  @Test
  void nanIdentityPartitionValueHasNoBounds() {
    PartitionField field = field(Types.FloatType.get(), b -> b.identity(COLUMN));

    assertThat(StatsUtil.sourceBounds(schema(Types.FloatType.get()), field, Float.NaN)).isNull();
  }

  @Test
  void voidFieldHasNoBounds() {
    PartitionField field = field(Types.StringType.get(), b -> b.alwaysNull(COLUMN));

    assertThat(StatsUtil.sourceBounds(schema(Types.StringType.get()), field, null)).isNull();
  }

  @Test
  void bucketFieldIsRejected() {
    PartitionField field = field(Types.StringType.get(), b -> b.bucket(COLUMN, 16));

    assertThatThrownBy(() -> StatsUtil.sourceBounds(schema(Types.StringType.get()), field, 3))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("bucket");
  }

  private static Schema schema(Case testCase) {
    return schema(testCase.type());
  }

  private static Schema schema(Type type) {
    return new Schema(Types.NestedField.optional(1, COLUMN, type));
  }

  private static PartitionField field(Case testCase) {
    return field(testCase.type(), testCase.transform());
  }

  private static PartitionField field(Type type, UnaryOperator<PartitionSpec.Builder> transform) {
    return transform.apply(PartitionSpec.builderFor(schema(type))).build().fields().get(0);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Object apply(PartitionField field, Type type, Object value) {
    return ((Transform) field.transform()).bind(type).apply(value);
  }

  private static Object shift(Object value, int delta) {
    if (value instanceof Integer i) {
      return i + delta;
    } else if (value instanceof Long l) {
      return l + delta;
    } else if (value instanceof BigDecimal d) {
      return d.add(BigDecimal.valueOf(delta, d.scale()));
    }

    throw new UnsupportedOperationException("Cannot shift: " + value);
  }
}
