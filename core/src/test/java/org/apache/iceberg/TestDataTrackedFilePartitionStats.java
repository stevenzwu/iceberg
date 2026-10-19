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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

class TestDataTrackedFilePartitionStats {
  private static final int ID = 1;
  private static final int TS = 2;
  private static final int CATEGORY = 3;

  private static final Schema SCHEMA =
      new Schema(
          optional(ID, "id", Types.IntegerType.get()),
          optional(TS, "ts", Types.TimestampType.withZone()),
          optional(CATEGORY, "category", Types.StringType.get()));

  private static final long RECORD_COUNT = 100L;

  private static final LocalDate DAY_DATE = LocalDate.of(2026, 10, 15);
  private static final int DAY = DateTimeUtil.daysFromDate(DAY_DATE);
  private static final long DAY_START = startOfDay(DAY_DATE);
  private static final long DAY_END = startOfDay(DAY_DATE.plusDays(1)) - 1;
  private static final long MIDDAY = DAY_START + 12 * 60 * 60 * DateTimeUtil.MICROS_PER_SECOND;

  private static final Comparator<Object> STRINGS = Comparators.forType(Types.StringType.get());

  @Test
  void partitionValuesGiveBoundsAndCounts() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").day("ts").build();

    ContentStats stats = wrap(spec, file(spec, noMetrics(), "books", DAY));

    FieldStats<?> category = stats.statsFor(CATEGORY);
    assertThat(category.lowerBound()).usingComparator(STRINGS).isEqualTo("books");
    assertThat(category.upperBound()).usingComparator(STRINGS).isEqualTo("books");
    assertThat(category.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(category.nullValueCount()).isEqualTo(0L);

    FieldStats<?> ts = stats.statsFor(TS);
    assertThat(ts.lowerBound()).isEqualTo(DAY_START);
    assertThat(ts.upperBound()).isEqualTo(DAY_END);
    assertThat(ts.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(ts.nullValueCount()).isEqualTo(0L);
  }

  @Test
  void typeIncludesPartitionSourceColumns() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").day("ts").build();

    ContentStats stats = wrap(spec, file(spec, noMetrics(), "books", DAY));

    assertThat(stats.fieldStats())
        .extracting(FieldStats::fieldId)
        .containsExactlyInAnyOrder(TS, CATEGORY);
    assertThat(stats.type().fields())
        .isEqualTo(StatsUtil.statsReadSchema(SCHEMA, List.of(TS, CATEGORY)).fields());
  }

  @Test
  void fileBoundsInsidePartitionRangeAreKept() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").build();
    Metrics metrics = boundsMetrics(TS, Types.TimestampType.withZone(), MIDDAY, MIDDAY + 1000);

    FieldStats<?> ts = wrap(spec, file(spec, metrics, DAY)).statsFor(TS);

    assertThat(ts.lowerBound()).isEqualTo(MIDDAY);
    assertThat(ts.upperBound()).isEqualTo(MIDDAY + 1000);
  }

  @Test
  void looseFileBoundsAreTightenedToPartitionRange() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").build();
    Metrics metrics = boundsMetrics(CATEGORY, Types.StringType.get(), "b", "c");

    FieldStats<?> category = wrap(spec, file(spec, metrics, "books")).statsFor(CATEGORY);

    assertThat(category.lowerBound()).usingComparator(STRINGS).isEqualTo("books");
    assertThat(category.upperBound()).usingComparator(STRINGS).isEqualTo("books");
  }

  @Test
  void fileBoundsOutsidePartitionRangeDeferToPartition() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).day("ts").build();
    long otherDayStart = startOfDay(LocalDate.of(2026, 10, 20));
    Metrics metrics =
        boundsMetrics(TS, Types.TimestampType.withZone(), otherDayStart, otherDayStart + 1000);

    FieldStats<?> ts = wrap(spec, file(spec, metrics, DAY)).statsFor(TS);

    assertThat(ts.lowerBound()).isEqualTo(DAY_START);
    assertThat(ts.upperBound()).isEqualTo(DAY_END);
  }

  @Test
  void partitionCountsOverrideFileCounts() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").build();
    Metrics metrics =
        new Metrics(RECORD_COUNT, null, Map.of(CATEGORY, 7L), Map.of(CATEGORY, 3L), null);

    FieldStats<?> category = wrap(spec, file(spec, metrics, "books")).statsFor(CATEGORY);

    assertThat(category.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(category.nullValueCount()).isEqualTo(0L);
  }

  @Test
  void nullPartitionValueMeansAllValuesAreNull() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").build();

    FieldStats<?> category =
        wrap(spec, file(spec, noMetrics(), (Object) null)).statsFor(CATEGORY);

    assertThat(category.lowerBound()).isNull();
    assertThat(category.upperBound()).isNull();
    assertThat(category.valueCount()).isEqualTo(RECORD_COUNT);
    assertThat(category.nullValueCount()).isEqualTo(RECORD_COUNT);
  }

  @Test
  void partitionFieldsOnTheSameSourceColumnAreTightened() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA)
            .truncate("category", 2, "category_2")
            .truncate("category", 4, "category_4")
            .build();

    FieldStats<?> category =
        wrap(spec, file(spec, noMetrics(), "bo", "book")).statsFor(CATEGORY);

    assertThat(category.lowerBound()).usingComparator(STRINGS).isEqualTo("book");
    assertThat(category.upperBound()).usingComparator(STRINGS).isEqualTo("bool");
  }

  @Test
  void columnsWithoutPartitionFieldsKeepFileStats() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").build();
    Types.IntegerType intType = Types.IntegerType.get();
    Metrics metrics =
        new Metrics(
            RECORD_COUNT,
            null,
            Map.of(ID, 90L),
            Map.of(ID, 10L),
            null,
            Map.of(ID, Conversions.toByteBuffer(intType, 1)),
            Map.of(ID, Conversions.toByteBuffer(intType, 9)));

    FieldStats<?> id = wrap(spec, file(spec, metrics, "books")).statsFor(ID);

    assertThat(id.lowerBound()).isEqualTo(1);
    assertThat(id.upperBound()).isEqualTo(9);
    assertThat(id.valueCount()).isEqualTo(90L);
    assertThat(id.nullValueCount()).isEqualTo(10L);
  }

  @Test
  void bucketPartitionFieldIsRejected() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("category", 16).build();
    DataFile file = file(spec, noMetrics(), 3);

    assertThatThrownBy(() -> wrap(spec, file))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("bucket");
  }

  @Test
  void voidPartitionFieldHasNoStats() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).alwaysNull("category").build();

    assertThat(wrap(spec, file(spec, noMetrics(), (Object) null))).isNull();
  }

  @Test
  void eachFileUsesItsOwnSpec() {
    PartitionSpec categorySpec =
        PartitionSpec.builderFor(SCHEMA).identity("category").withSpecId(0).build();
    PartitionSpec daySpec = PartitionSpec.builderFor(SCHEMA).day("ts").withSpecId(1).build();
    TrackedFileAdapters.DataTrackedFile adapter =
        TrackedFileAdapters.forDataFile(
            SCHEMA, Map.of(categorySpec.specId(), categorySpec, daySpec.specId(), daySpec));

    ContentStats categoryStats =
        adapter.wrap(file(categorySpec, noMetrics(), "books")).contentStats();
    assertThat(categoryStats.fieldStats()).extracting(FieldStats::fieldId).containsExactly(CATEGORY);

    ContentStats dayStats = adapter.wrap(file(daySpec, noMetrics(), DAY)).contentStats();
    assertThat(dayStats.fieldStats()).extracting(FieldStats::fieldId).containsExactly(TS);
    assertThat(dayStats.statsFor(TS).lowerBound()).isEqualTo(DAY_START);
    assertThat(dayStats.statsFor(TS).upperBound()).isEqualTo(DAY_END);
  }

  @Test
  void sourceColumnMissingFromTableSchemaIsSkipped() {
    Schema historicalSchema =
        new Schema(
            optional(ID, "id", Types.IntegerType.get()),
            optional(TS, "ts", Types.TimestampType.withZone()),
            optional(CATEGORY, "category", Types.StringType.get()),
            optional(4, "region", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(historicalSchema).identity("region").build();

    assertThat(wrap(spec, file(spec, noMetrics(), "west"))).isNull();
  }

  @Test
  void partitionStatsDoNotCarryOverToTheNextFile() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("category").build();
    TrackedFileAdapters.DataTrackedFile adapter =
        TrackedFileAdapters.forDataFile(SCHEMA, Map.of(spec.specId(), spec));

    FieldStats<?> books =
        adapter.wrap(file(spec, noMetrics(), "books")).contentStats().statsFor(CATEGORY);
    assertThat(books.lowerBound()).usingComparator(STRINGS).isEqualTo("books");

    FieldStats<?> allNull =
        adapter.wrap(file(spec, noMetrics(), (Object) null)).contentStats().statsFor(CATEGORY);
    assertThat(allNull.lowerBound()).isNull();
    assertThat(allNull.upperBound()).isNull();
    assertThat(allNull.nullValueCount()).isEqualTo(RECORD_COUNT);
  }

  private static ContentStats wrap(PartitionSpec spec, DataFile file) {
    return TrackedFileAdapters.forDataFile(SCHEMA, Map.of(spec.specId(), spec))
        .wrap(file)
        .contentStats();
  }

  private static DataFile file(PartitionSpec spec, Metrics metrics, Object... values) {
    PartitionData partition = new PartitionData(spec.partitionType());
    for (int pos = 0; pos < values.length; pos += 1) {
      partition.set(pos, values[pos]);
    }

    return new GenericDataFile(
        spec.specId(),
        "s3://bucket/data/file.parquet",
        FileFormat.PARQUET,
        partition,
        1024L,
        metrics,
        null,
        List.of(0L),
        null,
        null);
  }

  private static Metrics noMetrics() {
    return new Metrics(RECORD_COUNT, null, null, null, null);
  }

  private static Metrics boundsMetrics(int fieldId, Type type, Object lower, Object upper) {
    return new Metrics(
        RECORD_COUNT,
        null,
        null,
        null,
        null,
        Map.of(fieldId, Conversions.toByteBuffer(type, lower)),
        Map.of(fieldId, Conversions.toByteBuffer(type, upper)));
  }

  private static long startOfDay(LocalDate date) {
    return DateTimeUtil.microsFromTimestamptz(date.atStartOfDay().atOffset(ZoneOffset.UTC));
  }
}
