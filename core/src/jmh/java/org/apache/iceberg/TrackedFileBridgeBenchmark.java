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
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.TrackedFileAdapters.DataTrackedFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares strategies for presenting a legacy {@link DataFile} as a v4 {@link TrackedFile} row on
 * the write path, once per manifest entry:
 *
 * <ol>
 *   <li><b>convert</b> — materialize a fresh {@link TrackedFileStruct} every row, copying the
 *       file's key metadata, split offsets, and equality ids into the struct, then read every
 *       field.
 *   <li><b>wrap</b> — the production reusable {@link TrackedFileAdapters.DataTrackedFile},
 *       allocated once and re-pointed at the file per row (zero per-row allocation), then read
 *       every field.
 * </ol>
 *
 * <p>Both variants drive the same downstream reads: every top-level {@link StructLike} field
 * ordinal plus one level of descent into the nested {@code tracking} and {@code partition} structs,
 * matching how a manifest writer serializes an entry. This isolates the per-row envelope
 * construction cost (struct allocation plus array copies) rather than any serialization I/O —
 * nothing is written to a manifest file.
 *
 * <p>Content stats are intentionally left null here; the stats-production cost is measured
 * separately by {@link ContentStatsBridgeBenchmark}.
 *
 * <p>Run: {@code ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=TrackedFileBridgeBenchmark}
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TrackedFileBridgeBenchmark {

  private static final int FORMAT_VERSION = 4;
  private static final long SNAPSHOT_ID = 42L;
  private static final int NUM_SPLIT_OFFSETS = 1;

  private Schema schema;
  private Types.StructType partitionType;
  private DataFile dataFile;
  private Tracking tracking;

  // reusable production wrapper, allocated once, re-pointed at the file per invocation
  private DataTrackedFile reusableWrapper;

  @Setup(Level.Trial)
  public void setup() {
    this.schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "c1", Types.LongType.get()),
            Types.NestedField.optional(3, "c2", Types.StringType.get()),
            Types.NestedField.optional(4, "c3", Types.LongType.get()));

    PartitionSpec spec = PartitionSpec.unpartitioned();
    this.partitionType = spec.partitionType();

    List<Long> splitOffsets = Lists.newArrayListWithCapacity(NUM_SPLIT_OFFSETS);
    for (int i = 0; i < NUM_SPLIT_OFFSETS; i += 1) {
      splitOffsets.add((long) i * 128);
    }

    this.dataFile =
        new GenericDataFile(
            spec.specId(),
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            new PartitionData(partitionType),
            1_024L,
            new Metrics(1_000L, null, null, null, null),
            ByteBuffer.wrap(new byte[] {1, 2, 3, 4}),
            splitOffsets,
            0,
            null);
    this.tracking = TrackingBuilder.added(SNAPSHOT_ID).build();
    MetricsConfig metricsConfig =
        MetricsConfig.from(ImmutableMap.of(), schema, SortOrder.unsorted());
    this.reusableWrapper =
        TrackedFileAdapters.forDataFile(FORMAT_VERSION, schema, metricsConfig, partitionType);
  }

  @Benchmark
  public void convert(Blackhole blackhole) {
    TrackedFileStruct trackedFile =
        new TrackedFileStruct(
            tracking,
            FileContent.DATA,
            FORMAT_VERSION,
            dataFile.location(),
            dataFile.format(),
            (PartitionData) dataFile.partition(),
            dataFile.recordCount(),
            dataFile.fileSizeInBytes(),
            dataFile.specId(),
            null /* contentStats */,
            dataFile.sortOrderId(),
            null /* deletionVector */,
            null /* manifestInfo */,
            dataFile.keyMetadata(),
            dataFile.splitOffsets(),
            null /* equalityIds */);
    readAll(blackhole, trackedFile);
  }

  @Benchmark
  public void wrap(Blackhole blackhole) {
    DataTrackedFile trackedFile = reusableWrapper.wrap(dataFile, tracking);
    readAll(blackhole, trackedFile);
  }

  // Reads every top-level field and descends one level into nested structs, mirroring how a
  // manifest writer walks the entry when serializing.
  private void readAll(Blackhole blackhole, StructLike struct) {
    int size = struct.size();
    for (int pos = 0; pos < size; pos += 1) {
      Object value = struct.get(pos, Object.class);
      blackhole.consume(value);
      if (value instanceof StructLike child) {
        for (int childPos = 0; childPos < child.size(); childPos += 1) {
          blackhole.consume(child.get(childPos, Object.class));
        }
      }
    }
  }
}
