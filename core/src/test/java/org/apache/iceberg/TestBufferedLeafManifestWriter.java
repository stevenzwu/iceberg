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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link BufferedLeafManifestWriter}: the buffer-then-spill state machine, the
 * never-spilled / spilled-once / spilled-many partitioning across the {@link
 * BufferedLeafManifestWriter#closeAndTakeTail() tail} and {@link
 * BufferedLeafManifestWriter#toManifestFiles() spilled leaves}, and the alternative {@link
 * BufferedLeafManifestWriter#close() flush-as-final-leaf} terminal.
 */
public class TestBufferedLeafManifestWriter {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));
  // Partitioned spec avoids the Phase 2 empty-Parquet-row-group known issue in the unpartitioned
  // case (see TestV4SnapshotProducer for the same workaround).
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();
  private static final Types.StructType UNION_PARTITION_TYPE = SPEC.partitionType();
  private static final long SNAPSHOT_ID = 42L;
  // Small target + small avg so a handful of rows spills.
  private static final long TARGET_BYTES = 500L;
  private static final long AVG_BYTES_PER_ENTRY = 100L;

  @TempDir File tempDir;

  private final AtomicInteger leafCounter = new AtomicInteger();

  private Supplier<LeafManifestWriter> leafFactory() {
    return () -> {
      OutputFile out =
          Files.localOutput(
              new File(tempDir, "leaf-" + leafCounter.getAndIncrement() + ".parquet"));
      EncryptedOutputFile encrypted = EncryptedFiles.plainAsEncryptedOutput(out);
      return LeafManifestWriter.forData(
          SPEC, UNION_PARTITION_TYPE, encrypted, SNAPSHOT_ID, null, ImmutableMap.of());
    };
  }

  private TrackedFile row(int ordinal) {
    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-" + ordinal + ".parquet")
            .withFileSizeInBytes(1024L)
            .withPartitionPath("data_bucket=" + (ordinal % 16))
            .withRecordCount(10L)
            .build();
    Tracking tracking = TrackingBuilder.added(SNAPSHOT_ID).build();
    return TrackedFileAdapters.forDataFile(
            TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
            SCHEMA,
            MetricsConfig.from(ImmutableMap.of(), SCHEMA, SortOrder.unsorted()),
            UNION_PARTITION_TYPE)
        .wrap(dataFile, tracking);
  }

  @Test
  public void testNeverSpillsTailHoldsEverything() {
    // 4 rows × 100 = 400 < 500 target → nothing spills; the whole buffer is the tail.
    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    for (int i = 0; i < 4; i++) {
      writer.add(row(i));
    }
    List<TrackedFile> tail = writer.closeAndTakeTail();

    assertThat(tail).hasSize(4);
    assertThat(writer.toManifestFiles()).isEmpty();
    assertThat(leafCounter.get()).as("no leaf writers created").isEqualTo(0);
  }

  @Test
  public void testSpillsOnceWithTail() {
    // 6 rows × 100 = 600 → on the 5th add the projection hits 500 == target and the buffer flushes
    // one 5-row leaf; the 6th row is the sub-target tail.
    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    for (int i = 0; i < 6; i++) {
      writer.add(row(i));
    }
    List<TrackedFile> tail = writer.closeAndTakeTail();

    assertThat(tail).hasSize(1);
    assertThat(writer.toManifestFiles()).hasSize(1);
    assertThat(leafCounter.get()).isEqualTo(1);
  }

  @Test
  public void testSpillsMultipleTimesWithTail() {
    // 12 rows × 100 = 1200 → two 5-row leaves + a 2-row tail.
    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    for (int i = 0; i < 12; i++) {
      writer.add(row(i));
    }
    List<TrackedFile> tail = writer.closeAndTakeTail();

    assertThat(tail).hasSize(2);
    assertThat(writer.toManifestFiles()).hasSize(2);
    assertThat(leafCounter.get()).isEqualTo(2);
  }

  @Test
  public void testExactMultipleHasEmptyTail() {
    // 10 rows × 100 = 1000 → exactly two full leaves, empty tail.
    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    for (int i = 0; i < 10; i++) {
      writer.add(row(i));
    }
    List<TrackedFile> tail = writer.closeAndTakeTail();

    assertThat(tail).isEmpty();
    assertThat(writer.toManifestFiles()).hasSize(2);
  }

  @Test
  public void testCloseFlushesRemainderAsFinalLeaf() throws IOException {
    // close() (not closeAndTakeTail) flushes the sub-target remainder as a final leaf.
    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    for (int i = 0; i < 4; i++) {
      writer.add(row(i));
    }
    writer.close();

    assertThat(writer.toManifestFiles()).hasSize(1);
    assertThat(leafCounter.get()).isEqualTo(1);
  }

  @Test
  public void testBufferedRowCountAndEstimate() {
    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    writer.add(row(0));
    writer.add(row(1));
    assertThat(writer.bufferedRowCount()).isEqualTo(2);
    assertThat(writer.estimatedBufferedBytes()).isEqualTo(200L);
  }

  @Test
  public void testAddAfterCloseIsRejected() throws IOException {
    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    writer.close();
    assertThatThrownBy(() -> writer.add(row(0)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");
  }

  @Test
  public void testTakeTailTwiceIsRejected() {
    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    writer.add(row(0));
    writer.closeAndTakeTail();
    assertThatThrownBy(writer::closeAndTakeTail)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already closed");
  }

  @Test
  public void testToManifestFilesBeforeCloseIsRejected() {
    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    assertThatThrownBy(writer::toManifestFiles)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("unclosed");
  }

  @Test
  public void testConstructorRejectsInvalidArgs() {
    assertThatThrownBy(() -> new BufferedLeafManifestWriter(leafFactory(), 0L, AVG_BYTES_PER_ENTRY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("targetSizeBytes must be positive");
    assertThatThrownBy(() -> new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("avgBytesPerEntry must be positive");
  }

  @Test
  public void testAddMaterializesIndependentStatsFreeCopies() {
    // A single reusable write wrapper, re-pointed per row (the documented forDataFile usage). If
    // the
    // buffer retained the wrapper itself, both buffered rows would alias the last-wrapped file.
    // add() must materialize an independent, stats-free copy of each row.
    TrackedFileAdapters.DataTrackedFile wrapper =
        TrackedFileAdapters.forDataFile(
            TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
            SCHEMA,
            MetricsConfig.from(ImmutableMap.of(), SCHEMA, SortOrder.unsorted()),
            UNION_PARTITION_TYPE);
    Tracking tracking = TrackingBuilder.added(SNAPSHOT_ID).build();

    BufferedLeafManifestWriter writer =
        new BufferedLeafManifestWriter(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    writer.add(wrapper.wrap(dataFile("/path/to/a.parquet", 0), tracking));
    writer.add(wrapper.wrap(dataFile("/path/to/b.parquet", 1), tracking));

    List<TrackedFile> tail = writer.closeAndTakeTail();

    assertThat(tail)
        .as("buffered rows must not alias the reused wrapper")
        .extracting(TrackedFile::location)
        .containsExactly("/path/to/a.parquet", "/path/to/b.parquet");
    assertThat(tail)
        .allSatisfy(
            row -> {
              assertThat(row).isNotSameAs(wrapper);
              assertThat(row.contentStats()).as("retired rows drop column stats").isNull();
              assertThat(row.partition()).as("partition tuple is materialized").isNotNull();
            });
  }

  private DataFile dataFile(String path, int ordinal) {
    return DataFiles.builder(SPEC)
        .withPath(path)
        .withFileSizeInBytes(1024L)
        .withPartitionPath("data_bucket=" + (ordinal % 16))
        .withRecordCount(10L)
        .build();
  }
}
