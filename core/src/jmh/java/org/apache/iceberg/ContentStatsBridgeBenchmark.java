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
import org.apache.iceberg.TrackedFileAdapters.MapBackedContentStats;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
 * Compares strategies for exposing a legacy {@link ContentFile}'s stat maps (valueCounts,
 * nullValueCounts, nanValueCounts, lowerBounds, upperBounds) as v4 columnar stats on the write
 * path, once per manifest entry:
 *
 * <ol>
 *   <li><b>convert</b> — materialize a real {@link ContentStatsStruct} plus a {@link
 *       FieldStatsStruct} per column every row, then read every field.
 *   <li><b>wrap</b> — the production {@link TrackedFileAdapters.MapBackedContentStats}, a single
 *       reusable content wrapper re-pointed at the file's stat maps per row (zero per-row
 *       allocation), then read every field.
 *   <li><b>wrapContentConvertFields</b> — a reusable content wrapper (no per-row content object),
 *       but each column's {@link FieldStats} is materialized as a fresh {@link FieldStatsStruct} on
 *       access rather than served by a reusable map-backed view. Isolates whether the reusable
 *       field view (which holds references to the five stat maps) actually beats simply building a
 *       {@code FieldStatsStruct} per field.
 * </ol>
 *
 * <p>All variants are read back through the same positional {@link StructLike} walk: {@link
 * #readStruct} descends the content struct and each per-column {@code field_stats} child via {@code
 * get(pos)}, matching how a manifest writer serializes the entry. The delta isolates per-row
 * construction/allocation cost, not the byte-buffer decoding work that all must do.
 *
 * <p>Run: {@code ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ContentStatsBridgeBenchmark}
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ContentStatsBridgeBenchmark {

  @Param({"2", "10", "50", "200"})
  private int numColumns;

  private Schema schema;
  private Map<Integer, Long> valueCounts;
  private Map<Integer, Long> nullValueCounts;
  private Map<Integer, Long> nanValueCounts;
  private Map<Integer, ByteBuffer> lowerBounds;
  private Map<Integer, ByteBuffer> upperBounds;

  // stats read schema and per-column field structs, built once (a real convert writer would hoist
  // these too, since they depend only on the table schema, not per-row data)
  private Types.StructType statsReadSchema;
  private Map<Integer, Types.StructType> fieldStructs;

  // legacy content file whose stat maps the reusable production wrapper re-points at per invocation
  private DataFile dataFile;

  // reusable production wrapper, allocated once, re-pointed at the file per invocation
  private MapBackedContentStats reusableWrapper;

  // reusable content wrapper that materializes a FieldStatsStruct per column on access
  private HybridContentStats hybridWrapper;

  @Setup(Level.Trial)
  public void setup() {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(numColumns);
    for (int i = 0; i < numColumns; i++) {
      fields.add(Types.NestedField.optional(i + 1, "c" + i, Types.LongType.get()));
    }

    this.schema = new Schema(fields);
    this.valueCounts = Maps.newHashMap();
    this.nullValueCounts = Maps.newHashMap();
    this.nanValueCounts = Maps.newHashMap();
    this.lowerBounds = Maps.newHashMap();
    this.upperBounds = Maps.newHashMap();

    for (int i = 0; i < numColumns; i++) {
      int id = i + 1;
      valueCounts.put(id, 1_000L + i);
      nullValueCounts.put(id, (long) i);
      lowerBounds.put(id, Conversions.toByteBuffer(Types.LongType.get(), (long) i));
      upperBounds.put(id, Conversions.toByteBuffer(Types.LongType.get(), (long) (i + 1_000)));
    }

    PartitionSpec spec = PartitionSpec.unpartitioned();
    Metrics metrics =
        new Metrics(
            1_000L, null, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
    this.dataFile =
        new GenericDataFile(
            spec.specId(),
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            new PartitionData(spec.partitionType()),
            1_024L,
            metrics,
            null,
            null,
            null,
            null);
    // track full stats on every column (raise the inferred-column cap above the max numColumns) so
    // the reusable wrapper's stats schema matches the convert path's column count for a fair
    // compare
    MetricsConfig metricsConfig =
        MetricsConfig.from(
            ImmutableMap.of(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "1000"),
            schema,
            SortOrder.unsorted());
    this.reusableWrapper = new MapBackedContentStats(schema, metricsConfig);
    this.hybridWrapper = new HybridContentStats(schema);

    this.statsReadSchema = StatsUtil.statsReadSchema(schema, valueCounts.keySet());
    this.fieldStructs = Maps.newHashMap();
    for (int id = 1; id <= numColumns; id++) {
      fieldStructs.put(id, statsReadSchema.field(StatsUtil.toBaseId(id)).type().asStructType());
    }
  }

  @Benchmark
  public void convert(Blackhole blackhole) {
    readStruct(blackhole, buildContentStats());
  }

  @Benchmark
  public void wrap(Blackhole blackhole) {
    readStruct(blackhole, reusableWrapper.wrap(dataFile));
  }

  @Benchmark
  public void wrapContentConvertFields(Blackhole blackhole) {
    readStruct(
        blackhole,
        hybridWrapper.wrap(valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds));
  }

  // Walks the content struct positionally and recurses into each per-column field_stats child via
  // get(pos), mirroring how a manifest writer serializes the entry.
  private void readStruct(Blackhole blackhole, StructLike struct) {
    int size = struct.size();
    for (int pos = 0; pos < size; pos += 1) {
      Object value = struct.get(pos, Object.class);
      blackhole.consume(value);
      if (value instanceof StructLike child) {
        readStruct(blackhole, child);
      }
    }
  }

  // Materializes a real ContentStatsStruct + FieldStatsStruct per row, decoding every bound. The
  // stats schema and per-column field structs are precomputed once in setup, so this measures only
  // the per-row allocation and bound decoding, not the schema walk.
  private ContentStatsStruct buildContentStats() {
    ContentStatsStruct stats = new ContentStatsStruct(statsReadSchema);
    for (int id = 1; id <= numColumns; id++) {
      Types.StructType fieldStruct = fieldStructs.get(id);
      Type type = schema.findType(id);
      Object lower =
          lowerBounds.containsKey(id)
              ? Conversions.fromByteBuffer(type, lowerBounds.get(id))
              : null;
      Object upper =
          upperBounds.containsKey(id)
              ? Conversions.fromByteBuffer(type, upperBounds.get(id))
              : null;
      stats.setStats(
          id,
          new FieldStatsStruct<>(
              fieldStruct,
              lower,
              upper,
              false,
              valueCounts.getOrDefault(id, 0L),
              nullValueCounts.getOrDefault(id, 0L),
              nanValueCounts.getOrDefault(id, 0L),
              null));
    }

    return stats;
  }

  /**
   * Reusable content wrapper that materializes a {@link FieldStatsStruct} per column on access.
   *
   * <p>The content-level container (schema, per-column sub-struct types) is allocated once and the
   * stat maps are re-pointed per row, but {@link #statsFor} builds a fresh {@code FieldStatsStruct}
   * for the requested column rather than serving a reusable map-backed view.
   */
  private static final class HybridContentStats implements StructLike {
    private final Schema schema;
    private final Types.StructType struct;
    private final int[] posToId;
    private final Map<Integer, Types.StructType> fieldStructs;

    private Map<Integer, Long> valueCounts;
    private Map<Integer, Long> nullValueCounts;
    private Map<Integer, Long> nanValueCounts;
    private Map<Integer, ByteBuffer> lowerBounds;
    private Map<Integer, ByteBuffer> upperBounds;

    HybridContentStats(Schema schema) {
      this.schema = schema;
      this.fieldStructs = Maps.newHashMap();
      List<Integer> ids = Lists.newArrayList();
      for (Types.NestedField field : schema.columns()) {
        ids.add(field.fieldId());
      }

      this.struct = StatsUtil.statsReadSchema(schema, ids);
      List<Types.NestedField> fields = struct.fields();
      this.posToId = new int[fields.size()];
      for (int i = 0; i < fields.size(); i += 1) {
        Types.NestedField field = fields.get(i);
        int fieldId = StatsUtil.toFieldId(field.fieldId());
        posToId[i] = fieldId;
        fieldStructs.put(fieldId, field.type().asStructType());
      }
    }

    HybridContentStats wrap(
        Map<Integer, Long> newValueCounts,
        Map<Integer, Long> newNullValueCounts,
        Map<Integer, Long> newNanValueCounts,
        Map<Integer, ByteBuffer> newLowerBounds,
        Map<Integer, ByteBuffer> newUpperBounds) {
      this.valueCounts = newValueCounts;
      this.nullValueCounts = newNullValueCounts;
      this.nanValueCounts = newNanValueCounts;
      this.lowerBounds = newLowerBounds;
      this.upperBounds = newUpperBounds;
      return this;
    }

    FieldStats<?> statsFor(int id) {
      Type type = schema.findType(id);
      Object lower =
          lowerBounds.containsKey(id)
              ? Conversions.fromByteBuffer(type, lowerBounds.get(id))
              : null;
      Object upper =
          upperBounds.containsKey(id)
              ? Conversions.fromByteBuffer(type, upperBounds.get(id))
              : null;
      return new FieldStatsStruct<>(
          fieldStructs.get(id),
          lower,
          upper,
          false,
          count(valueCounts, id),
          count(nullValueCounts, id),
          count(nanValueCounts, id),
          null);
    }

    private static long count(Map<Integer, Long> counts, int id) {
      Long value = counts.get(id);
      return value == null ? 0L : value;
    }

    @Override
    public int size() {
      return struct.fields().size();
    }

    @Override
    public <C> C get(int pos, Class<C> javaClass) {
      return javaClass.cast(statsFor(posToId[pos]));
    }

    @Override
    public <C> void set(int pos, C value) {
      throw new UnsupportedOperationException("read-only benchmark view");
    }
  }
}
