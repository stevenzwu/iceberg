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
import org.apache.iceberg.TrackedFileAdapters.DataTrackedFile;
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
 * Measures the combined per-row cost of presenting a legacy {@link DataFile} as a full v4 {@link
 * TrackedFile} row — envelope plus real per-column content stats — once per manifest entry, so the
 * two cost sources measured in isolation by {@link TrackedFileBridgeBenchmark} (envelope only) and
 * {@link ContentStatsBridgeBenchmark} (stats only) can be seen together as column count scales.
 *
 * <p>Three strategies, parameterized on column count:
 *
 * <ol>
 *   <li><b>convertBoth</b> — materialize a fresh {@link TrackedFileStruct} envelope every row and a
 *       fresh {@link ContentStatsStruct} (plus a {@link FieldStatsStruct} per column) for its
 *       stats.
 *   <li><b>wrapBoth</b> — the production reusable {@link DataTrackedFile}, allocated once and
 *       re-pointed at the file per row; its content stats are served by a reusable {@link
 *       MapBackedContentStats} view (zero per-row allocation for either envelope or stats).
 *   <li><b>convertEnvelopeWrapStats</b> — the hybrid: a fresh {@link TrackedFileStruct} envelope
 *       per row, but its stats are a reusable {@link MapBackedContentStats} re-pointed at the file
 *       rather than a freshly built struct.
 * </ol>
 *
 * <p>All variants are read back through the same recursive positional {@link StructLike} walk:
 * {@link #readAll} descends every top-level ordinal and recurses into the nested tracking,
 * partition, and content-stats structs (and each per-column {@code field_stats} child), matching
 * how a manifest writer serializes the entry. The delta isolates per-row construction/allocation
 * cost, not the byte-buffer decoding work that all variants must do once per column per row.
 *
 * <p>Run: {@code ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=TrackedFileWithStatsBenchmark}
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TrackedFileWithStatsBenchmark {

  private static final int FORMAT_VERSION = 4;
  private static final long SNAPSHOT_ID = 42L;
  private static final int NUM_SPLIT_OFFSETS = 4;

  @Param({"2", "10", "50", "200"})
  private int numColumns;

  private Schema schema;
  private Types.StructType partitionType;
  private Map<Integer, Long> valueCounts;
  private Map<Integer, Long> nullValueCounts;
  private Map<Integer, Long> nanValueCounts;
  private Map<Integer, ByteBuffer> lowerBounds;
  private Map<Integer, ByteBuffer> upperBounds;

  // stats read schema and per-column field structs, built once (a real convert writer would hoist
  // these too, since they depend only on the table schema, not per-row data)
  private Types.StructType statsReadSchema;
  private Map<Integer, Types.StructType> fieldStructs;

  private DataFile dataFile;
  private Tracking tracking;

  // reusable production envelope+stats wrapper, allocated once, re-pointed per invocation
  private DataTrackedFile reusableWrapper;

  // reusable stats-only wrapper used by the hybrid path (fresh envelope, reused stats)
  private MapBackedContentStats reusableStats;

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
    this.partitionType = spec.partitionType();

    List<Long> splitOffsets = Lists.newArrayListWithCapacity(NUM_SPLIT_OFFSETS);
    for (int i = 0; i < NUM_SPLIT_OFFSETS; i += 1) {
      splitOffsets.add((long) i * 128);
    }

    Metrics metrics =
        new Metrics(
            1_000L, null, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
    this.dataFile =
        new GenericDataFile(
            spec.specId(),
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            new PartitionData(partitionType),
            1_024L,
            metrics,
            ByteBuffer.wrap(new byte[] {1, 2, 3, 4}),
            splitOffsets,
            0,
            null);
    this.tracking = TrackingBuilder.added(SNAPSHOT_ID).build();
    // track full stats on every column (raise the inferred-column cap above the max numColumns) so
    // the reusable wrapper's stats schema matches the convert path's column count for a fair
    // compare
    MetricsConfig metricsConfig =
        MetricsConfig.from(
            ImmutableMap.of(TableProperties.METRICS_MAX_INFERRED_COLUMN_DEFAULTS, "1000"),
            schema,
            SortOrder.unsorted());
    this.reusableWrapper =
        TrackedFileAdapters.forDataFile(FORMAT_VERSION, schema, metricsConfig, partitionType);
    this.reusableStats = new MapBackedContentStats(schema, metricsConfig);

    this.statsReadSchema = StatsUtil.statsReadSchema(schema, valueCounts.keySet());
    this.fieldStructs = Maps.newHashMap();
    for (int id = 1; id <= numColumns; id++) {
      fieldStructs.put(id, statsReadSchema.field(StatsUtil.toBaseId(id)).type().asStructType());
    }
  }

  @Benchmark
  public void convertBoth(Blackhole blackhole) {
    readAll(blackhole, newEnvelope(buildContentStats()));
  }

  @Benchmark
  public void wrapBoth(Blackhole blackhole) {
    readAll(blackhole, reusableWrapper.wrap(dataFile, tracking));
  }

  @Benchmark
  public void convertEnvelopeWrapStats(Blackhole blackhole) {
    readAll(blackhole, newEnvelope(reusableStats.wrap(dataFile)));
  }

  // Materializes a fresh TrackedFileStruct envelope carrying the supplied content stats.
  private TrackedFileStruct newEnvelope(ContentStats contentStats) {
    return new TrackedFileStruct(
        tracking,
        FileContent.DATA,
        FORMAT_VERSION,
        dataFile.location(),
        dataFile.format(),
        (PartitionData) dataFile.partition(),
        dataFile.recordCount(),
        dataFile.fileSizeInBytes(),
        dataFile.specId(),
        contentStats,
        dataFile.sortOrderId(),
        null /* deletionVector */,
        null /* manifestInfo */,
        dataFile.keyMetadata(),
        dataFile.splitOffsets(),
        null /* equalityIds */);
  }

  // Materializes a real ContentStatsStruct + FieldStatsStruct per column, decoding every bound. The
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

  // Reads every top-level ordinal and recurses into every nested struct (tracking, partition,
  // content stats, and each per-column field_stats child), mirroring manifest serialization.
  private void readAll(Blackhole blackhole, StructLike struct) {
    int size = struct.size();
    for (int pos = 0; pos < size; pos += 1) {
      Object value = struct.get(pos, Object.class);
      blackhole.consume(value);
      if (value instanceof StructLike child) {
        readAll(blackhole, child);
      }
    }
  }
}
