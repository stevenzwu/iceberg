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
import java.util.List;
import java.util.Map;
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
 * Compares strategies for exposing a v4 manifest entry's {@link ContentStats} as the legacy {@link
 * ContentFile} stat maps (valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds),
 * once per manifest entry.
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
@Fork(10)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ContentStatsReadBenchmark {

  // rotate a mix of column types so the bound encoding is polymorphic, not a single Long path
  private static final Type[] COLUMN_TYPES = {
    Types.BooleanType.get(),
    Types.IntegerType.get(),
    Types.LongType.get(),
    Types.FloatType.get(),
    Types.DoubleType.get(),
    Types.StringType.get(),
    Types.DateType.get(),
    Types.TimestampType.withoutZone(),
  };

  @Param({"20", "100"})
  private int numColumns;

  private Schema schema;

  // real v4 content stats for one manifest entry, deserialized once (as a manifest reader would),
  // then presented through the legacy map API on every op
  private ContentStats sourceStats;

  @Setup(Level.Trial)
  public void setupBenchmark() {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(numColumns);
    for (int i = 0; i < numColumns; i++) {
      fields.add(Types.NestedField.optional(i + 1, "c" + i, COLUMN_TYPES[i % COLUMN_TYPES.length]));
    }

    this.schema = new Schema(fields);

    List<Integer> ids = Lists.newArrayListWithCapacity(numColumns);
    for (Types.NestedField field : schema.columns()) {
      ids.add(field.fieldId());
    }

    Types.StructType statsReadSchema = StatsUtil.statsReadSchema(schema, ids);
    ContentStatsStruct stats = new ContentStatsStruct(statsReadSchema);
    for (int id = 1; id <= numColumns; id++) {
      Type type = schema.findType(id);
      Types.StructType fieldStruct =
          statsReadSchema.field(StatsUtil.toBaseId(id)).type().asStructType();
      boolean floating = type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE;
      Long nanValueCount = floating ? 0L : null;
      stats.setStats(
          id,
          new FieldStatsStruct<>(
              fieldStruct,
              lowerBoundFor(type, id),
              upperBoundFor(type, id),
              false,
              1_000L + id,
              (long) id,
              nanValueCount,
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
      if (fieldStats.hasNullCount()) {
        nullValueCounts.put(id, fieldStats.nullValueCount());
      }

      if (fieldStats.hasNaNCount()) {
        nanValueCounts.put(id, fieldStats.nanValueCount());
      }

      lowerBounds.put(id, Conversions.toByteBuffer(type, fieldStats.lowerBound()));
      upperBounds.put(id, Conversions.toByteBuffer(type, fieldStats.upperBound()));
    }

    readMaps(blackhole, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
  }

  @Benchmark
  public void wrap(Blackhole blackhole) {
    Map<Integer, Long> valueCounts =
        new ContentStatsBackedMap<>(sourceStats, ContentStatsBackedMap.Kind.VALUE_COUNT);
    Map<Integer, Long> nullValueCounts =
        new ContentStatsBackedMap<>(sourceStats, ContentStatsBackedMap.Kind.NULL_VALUE_COUNT);
    Map<Integer, Long> nanValueCounts =
        new ContentStatsBackedMap<>(sourceStats, ContentStatsBackedMap.Kind.NAN_VALUE_COUNT);
    Map<Integer, ByteBuffer> lowerBounds =
        new ContentStatsBackedMap<>(sourceStats, ContentStatsBackedMap.Kind.LOWER_BOUND);
    Map<Integer, ByteBuffer> upperBounds =
        new ContentStatsBackedMap<>(sourceStats, ContentStatsBackedMap.Kind.UPPER_BOUND);

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

  private static Object lowerBoundFor(Type type, int id) {
    switch (type.typeId()) {
      case BOOLEAN:
        return false;
      case INTEGER:
      case DATE:
        return id;
      case LONG:
      case TIMESTAMP:
        return (long) id;
      case FLOAT:
        return (float) id;
      case DOUBLE:
        return (double) id;
      case STRING:
        return "lo" + id;
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  private static Object upperBoundFor(Type type, int id) {
    switch (type.typeId()) {
      case BOOLEAN:
        return true;
      case INTEGER:
      case DATE:
        return id + 1_000;
      case LONG:
      case TIMESTAMP:
        return (long) (id + 1_000);
      case FLOAT:
        return (float) (id + 1_000);
      case DOUBLE:
        return (double) (id + 1_000);
      case STRING:
        return "up" + id;
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
}
