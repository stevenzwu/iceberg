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

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Read-path mirror of {@link ContentStatsBridgeBenchmark}: compares strategies for exposing a v4
 * manifest entry's {@link ContentStats} as the legacy {@link ContentFile} stat maps (valueCounts,
 * nullValueCounts, nanValueCounts, lowerBounds, upperBounds), once per manifest entry.
 *
 * <p>A read-path {@code TrackedContentFile} must present the columnar v4 stats through the legacy
 * five-map API that scan planning and metrics evaluators consume. Two strategies:
 *
 * <ol>
 *   <li><b>convert</b> — materialize five real {@link Map}s from the content stats every row (what
 *       a {@code TrackedContentFile} does today), then read every column.
 *   <li><b>wrap</b> — five reusable {@link ContentStatsBackedMap} views backed directly by the
 *       content stats, computing each entry lazily on {@code get(fieldId)} with no per-row map
 *       allocation, then read every column.
 * </ol>
 *
 * <p>The read walk is held identical across variants: {@link #readMaps} looks up all five stats for
 * every column by field id, matching how a metrics evaluator probes bounds and counts per column.
 * Because {@link FieldStats} exposes decoded bounds while the legacy API returns {@link
 * ByteBuffer}, both variants re-encode bounds via {@link Conversions#toByteBuffer} on the same
 * access — so the measured delta isolates map materialization versus a lazy view, not the encode
 * work both share.
 *
 * <p>Run: {@code ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ContentStatsReadBenchmark}
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ContentStatsReadBenchmark {

  @Param({"2", "10", "50", "200"})
  private int numColumns;

  private Schema schema;

  // real v4 content stats for one manifest entry, deserialized once (as a manifest reader would),
  // then presented through the legacy map API on every op
  private ContentStats sourceStats;

  @Setup(Level.Trial)
  public void setup() {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(numColumns);
    for (int i = 0; i < numColumns; i++) {
      fields.add(Types.NestedField.optional(i + 1, "c" + i, Types.LongType.get()));
    }

    this.schema = new Schema(fields);

    List<Integer> ids = Lists.newArrayListWithCapacity(numColumns);
    for (Types.NestedField field : schema.columns()) {
      ids.add(field.fieldId());
    }

    Types.StructType statsReadSchema = StatsUtil.statsReadSchema(schema, ids);
    ContentStatsStruct stats = new ContentStatsStruct(statsReadSchema);
    for (int id = 1; id <= numColumns; id++) {
      Types.StructType fieldStruct =
          statsReadSchema.field(StatsUtil.toBaseId(id)).type().asStructType();
      stats.setStats(
          id,
          new FieldStatsStruct<>(
              fieldStruct,
              (long) id,
              (long) (id + 1_000),
              false,
              1_000L + id,
              (long) id,
              0L,
              null));
    }

    this.sourceStats = stats;
  }

  @Benchmark
  public void convert(Blackhole blackhole) {
    Map<Integer, Long> valueCounts = Maps.newHashMapWithExpectedSize(numColumns);
    Map<Integer, Long> nullValueCounts = Maps.newHashMapWithExpectedSize(numColumns);
    Map<Integer, Long> nanValueCounts = Maps.newHashMapWithExpectedSize(numColumns);
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMapWithExpectedSize(numColumns);
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMapWithExpectedSize(numColumns);

    for (FieldStats<?> fieldStats : sourceStats.fieldStats()) {
      int id = fieldStats.fieldId();
      Type type = schema.findType(id);
      valueCounts.put(id, fieldStats.valueCount());
      nullValueCounts.put(id, fieldStats.nullValueCount());
      nanValueCounts.put(id, fieldStats.nanValueCount());
      lowerBounds.put(id, Conversions.toByteBuffer(type, fieldStats.lowerBound()));
      upperBounds.put(id, Conversions.toByteBuffer(type, fieldStats.upperBound()));
    }

    readMaps(blackhole, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
  }

  @Benchmark
  public void wrap(Blackhole blackhole) {
    Map<Integer, Long> valueCounts =
        new ContentStatsBackedMap<>(sourceStats, schema, ContentStatsBackedMap.Kind.VALUE_COUNT);
    Map<Integer, Long> nullValueCounts =
        new ContentStatsBackedMap<>(sourceStats, schema, ContentStatsBackedMap.Kind.NULL_COUNT);
    Map<Integer, Long> nanValueCounts =
        new ContentStatsBackedMap<>(sourceStats, schema, ContentStatsBackedMap.Kind.NAN_COUNT);
    Map<Integer, ByteBuffer> lowerBounds =
        new ContentStatsBackedMap<>(sourceStats, schema, ContentStatsBackedMap.Kind.LOWER_BOUND);
    Map<Integer, ByteBuffer> upperBounds =
        new ContentStatsBackedMap<>(sourceStats, schema, ContentStatsBackedMap.Kind.UPPER_BOUND);

    readMaps(blackhole, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
  }

  // Probes all five stats for every column by field id, mirroring how a metrics evaluator reads
  // per-column bounds and counts from a legacy ContentFile.
  private void readMaps(
      Blackhole blackhole,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Map<Integer, Long> nanValueCounts,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    for (int id = 1; id <= numColumns; id++) {
      blackhole.consume(valueCounts.get(id));
      blackhole.consume(nullValueCounts.get(id));
      blackhole.consume(nanValueCounts.get(id));
      blackhole.consume(lowerBounds.get(id));
      blackhole.consume(upperBounds.get(id));
    }
  }

  /**
   * A read-only legacy stat map ({@code Map<Integer, V>}) backed by {@link ContentStats}. Each
   * {@link #get(Object)} resolves the field stats for the requested column id and projects the one
   * stat this map exposes, decoding/encoding lazily and allocating nothing per row.
   */
  static final class ContentStatsBackedMap<V> extends AbstractMap<Integer, V> {
    enum Kind {
      VALUE_COUNT,
      NULL_COUNT,
      NAN_COUNT,
      LOWER_BOUND,
      UPPER_BOUND
    }

    private final ContentStats stats;
    private final Schema schema;
    private final Kind kind;

    ContentStatsBackedMap(ContentStats stats, Schema schema, Kind kind) {
      this.stats = stats;
      this.schema = schema;
      this.kind = kind;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
      if (!(key instanceof Integer id)) {
        return null;
      }

      FieldStats<?> fieldStats = stats.statsFor(id);
      if (fieldStats == null) {
        return null;
      }

      return switch (kind) {
        case VALUE_COUNT -> (V) Long.valueOf(fieldStats.valueCount());
        case NULL_COUNT -> (V) Long.valueOf(fieldStats.nullValueCount());
        case NAN_COUNT -> (V) Long.valueOf(fieldStats.nanValueCount());
        case LOWER_BOUND ->
            (V) Conversions.toByteBuffer(schema.findType(id), fieldStats.lowerBound());
        case UPPER_BOUND ->
            (V) Conversions.toByteBuffer(schema.findType(id), fieldStats.upperBound());
      };
    }

    @Override
    public Set<Entry<Integer, V>> entrySet() {
      Map<Integer, V> materialized = Maps.newLinkedHashMap();
      for (FieldStats<?> fieldStats : stats.fieldStats()) {
        int id = fieldStats.fieldId();
        materialized.put(id, get(id));
      }

      return materialized.entrySet();
    }
  }
}
