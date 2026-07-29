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
 * Unit tests for {@link V4WritePool}: verify the buffer-then-spill state machine, the never-spilled
 * / spilled-once / spilled-many partitioning of rows across {@code rootDirectRows()} and
 * {@code leafManifests()}, and the {@code leafRowStatus} echo.
 */
public class TestV4WritePool {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()));
  // Partitioned spec avoids the Phase 2 empty-Parquet-row-group known issue in the unpartitioned
  // case (see TestV4SnapshotProducer for the same workaround).
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();
  private static final Types.StructType UNION_PARTITION_TYPE = SPEC.partitionType();
  private static final long SNAPSHOT_ID = 42L;
  // Small target + small avg so we can spill with a handful of rows.
  private static final long TARGET_BYTES = 500L;
  private static final double AVG_BYTES_PER_ENTRY = 100.0;

  @TempDir File tempDir;

  private final AtomicInteger leafCounter = new AtomicInteger();

  private Supplier<TrackedFileWriter> leafFactory() {
    return () -> {
      OutputFile out =
          Files.localOutput(
              new File(tempDir, "leaf-" + leafCounter.getAndIncrement() + ".parquet"));
      EncryptedOutputFile encrypted = EncryptedFiles.plainAsEncryptedOutput(out);
      return TrackedFileWriter.forDataLeaf(
          SPEC, UNION_PARTITION_TYPE, encrypted, SNAPSHOT_ID, null, ImmutableMap.of());
    };
  }

  private TrackedFile row(int i) {
    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-" + i + ".parquet")
            .withFileSizeInBytes(1024L)
            .withPartitionPath("data_bucket=" + (i % 16))
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
  public void testNeverSpills() {
    // 4 rows * 100 bytes/entry = 400 < 500 target -> nothing spills.
    V4WritePool pool =
        new V4WritePool(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, EntryStatus.ADDED);
    for (int i = 0; i < 4; i++) {
      pool.add(row(i));
    }
    pool.close();

    assertThat(pool.spilled()).isFalse();
    assertThat(pool.rootDirectRows()).hasSize(4);
    assertThat(pool.leafManifests()).isEmpty();
    assertThat(pool.leafRowStatus()).isEqualTo(EntryStatus.ADDED);
    assertThat(leafCounter.get()).as("no leaf writers created").isEqualTo(0);
  }

  @Test
  public void testSpillsOnceWithTail() {
    // 6 rows * 100 = 600 bytes projected. On the 5th add, the projection reaches 500 == target so
    // the buffer drains through a RollingTrackedFileWriter: the drain fills exactly one target-sized
    // leaf (5 rows) and the 6th row is the sub-target tail.
    V4WritePool pool =
        new V4WritePool(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, EntryStatus.ADDED);
    for (int i = 0; i < 6; i++) {
      pool.add(row(i));
    }
    pool.close();

    assertThat(pool.spilled()).isTrue();
    assertThat(pool.leafManifests()).hasSize(1);
    assertThat(pool.rootDirectRows()).hasSize(1);
    // Every row is accounted for exactly once across leaves + root remainder.
    long leafRecords =
        pool.leafManifests().stream()
            .mapToLong(m -> m.addedFilesCount() != null ? m.addedFilesCount() : 0)
            .sum();
    assertThat(leafRecords + pool.rootDirectRows().size()).isEqualTo(6);
  }

  @Test
  public void testSpillsMultipleTimes() {
    // 12 rows * 100 = 1200 bytes projected -> two full target-sized leaves (5 rows each) plus 2 tail
    // rows kept for the root.
    V4WritePool pool =
        new V4WritePool(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, EntryStatus.ADDED);
    for (int i = 0; i < 12; i++) {
      pool.add(row(i));
    }
    pool.close();

    assertThat(pool.spilled()).isTrue();
    assertThat(pool.leafManifests()).hasSize(2);
    assertThat(pool.rootDirectRows()).hasSize(2);
    // Confirm every row landed exactly once.
    long leafRecords =
        pool.leafManifests().stream()
            .mapToLong(m -> m.addedFilesCount() != null ? m.addedFilesCount() : 0)
            .sum();
    assertThat(leafRecords + pool.rootDirectRows().size()).isEqualTo(12);
    // Two leaves means the factory was invoked twice.
    assertThat(leafCounter.get()).isEqualTo(2);
  }

  @Test
  public void testExactMultipleHasEmptyTail() {
    // 10 rows * 100 = 1000 bytes -> exactly two full leaves, empty tail.
    V4WritePool pool =
        new V4WritePool(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, EntryStatus.ADDED);
    for (int i = 0; i < 10; i++) {
      pool.add(row(i));
    }
    pool.close();

    assertThat(pool.spilled()).isTrue();
    assertThat(pool.leafManifests()).hasSize(2);
    assertThat(pool.rootDirectRows()).isEmpty();
  }

  @Test
  public void testLeafRowStatusEcho() {
    for (EntryStatus status :
        new EntryStatus[] {EntryStatus.ADDED, EntryStatus.DELETED, EntryStatus.REPLACED}) {
      V4WritePool pool =
          new V4WritePool(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, status);
      assertThat(pool.leafRowStatus()).as("status echo").isEqualTo(status);
      pool.close();
      // even after close, echo still holds
      assertThat(pool.leafRowStatus()).isEqualTo(status);
    }
  }

  @Test
  public void testDoubleCloseIsIdempotent() {
    V4WritePool pool =
        new V4WritePool(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, EntryStatus.ADDED);
    pool.add(row(0));
    pool.close();
    List<TrackedFile> firstDirect = pool.rootDirectRows();
    pool.close();
    assertThat(pool.rootDirectRows()).isEqualTo(firstDirect);
  }

  @Test
  public void testAddAfterCloseIsRejected() {
    V4WritePool pool =
        new V4WritePool(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, EntryStatus.ADDED);
    pool.close();
    assertThatThrownBy(() -> pool.add(row(0)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");
  }

  @Test
  public void testGettersRejectPreClose() {
    V4WritePool pool =
        new V4WritePool(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, EntryStatus.ADDED);
    assertThatThrownBy(pool::spilled)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not closed");
    assertThatThrownBy(pool::rootDirectRows)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not closed");
    assertThatThrownBy(pool::leafManifests)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not closed");
  }

  @Test
  public void testConstructorRejectsInvalidArgs() {
    assertThatThrownBy(
            () -> new V4WritePool(null, TARGET_BYTES, AVG_BYTES_PER_ENTRY, EntryStatus.ADDED))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new V4WritePool(leafFactory(), 0L, AVG_BYTES_PER_ENTRY, EntryStatus.ADDED))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new V4WritePool(leafFactory(), TARGET_BYTES, 0.0, EntryStatus.ADDED))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new V4WritePool(leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

}
