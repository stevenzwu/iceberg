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
 * Unit tests for {@link StreamingLeafManifestWriter}: verify streaming behavior (writer opens on
 * first add, rolls at projection crossings) and promotion (the last open writer becomes an open
 * {@link RootManifestWriter} the caller drives and closes).
 */
public class TestStreamingLeafManifestWriter {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();
  private static final Types.StructType UNION_PARTITION_TYPE = SPEC.partitionType();
  private static final long SNAPSHOT_ID = 42L;
  private static final long COMMIT_SEQ_NUMBER = 1L;
  // Small target + small avg so 5 rows roll a leaf.
  private static final long TARGET_BYTES = 500L;
  private static final long AVG_BYTES_PER_ENTRY = 100L;

  @TempDir File tempDir;

  private final AtomicInteger writerCounter = new AtomicInteger();

  private Supplier<LeafManifestWriter> writerFactory() {
    return () -> {
      OutputFile out =
          Files.localOutput(
              new File(tempDir, "writer-" + writerCounter.getAndIncrement() + ".parquet"));
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
  public void testPromoteWithoutAnyAdds() throws IOException {
    StreamingLeafManifestWriter writer =
        new StreamingLeafManifestWriter(writerFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);

    RootManifestWriter root = writer.promoteCurrentToRoot(SNAPSHOT_ID, COMMIT_SEQ_NUMBER, 0L);
    root.close();

    // A writer is opened on the spot to hold zero rows and become the root manifest.
    assertThat(writerCounter.get()).isEqualTo(1);
    assertThat(writer.completedLeaves()).isEmpty();
    assertThat(root.toSnapshotFile().location()).isNotNull();
  }

  @Test
  public void testStreamsBelowTargetThenPromotes() throws IOException {
    StreamingLeafManifestWriter writer =
        new StreamingLeafManifestWriter(writerFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    // 4 rows × 100 = 400 < 500: stays in one writer, never rolls.
    for (int i = 0; i < 4; i++) {
      writer.add(row(i));
    }

    RootManifestWriter root = writer.promoteCurrentToRoot(SNAPSHOT_ID, COMMIT_SEQ_NUMBER, 0L);
    root.close();

    assertThat(writerCounter.get()).as("single writer opened").isEqualTo(1);
    assertThat(writer.completedLeaves()).as("nothing rolled").isEmpty();
    assertThat(root.toSnapshotFile().location()).isNotNull();
  }

  @Test
  public void testStreamsAndRollsThenPromotes() throws IOException {
    StreamingLeafManifestWriter writer =
        new StreamingLeafManifestWriter(writerFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    // 6 rows × 100: rolls after 5 (500 == target), 1 row remains in the next writer.
    for (int i = 0; i < 6; i++) {
      writer.add(row(i));
    }

    RootManifestWriter root = writer.promoteCurrentToRoot(SNAPSHOT_ID, COMMIT_SEQ_NUMBER, 0L);
    root.close();

    // First writer rolled as leaf at row 5; second writer opened for row 6 and became root.
    assertThat(writerCounter.get()).isEqualTo(2);
    assertThat(writer.completedLeaves()).as("one rolled leaf").hasSize(1);
    assertThat(root.toSnapshotFile().location()).isNotNull();
  }

  @Test
  public void testExactMultipleRollsThenPromoteOpensNewWriter() throws IOException {
    StreamingLeafManifestWriter writer =
        new StreamingLeafManifestWriter(writerFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    // 5 rows × 100 = 500 == target: the 5th add rolls, no writer open after.
    for (int i = 0; i < 5; i++) {
      writer.add(row(i));
    }

    RootManifestWriter root = writer.promoteCurrentToRoot(SNAPSHOT_ID, COMMIT_SEQ_NUMBER, 0L);
    root.close();

    // Promotion opens a fresh writer to hold zero rows and become root.
    assertThat(writerCounter.get()).isEqualTo(2);
    assertThat(writer.completedLeaves()).hasSize(1);
    assertThat(root.toSnapshotFile().location()).isNotNull();
  }

  @Test
  public void testAddAfterPromoteIsRejected() throws IOException {
    StreamingLeafManifestWriter writer =
        new StreamingLeafManifestWriter(writerFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    writer.promoteCurrentToRoot(SNAPSHOT_ID, COMMIT_SEQ_NUMBER, 0L).close();
    assertThatThrownBy(() -> writer.add(row(0)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("promoted");
  }

  @Test
  public void testDoublePromoteIsRejected() throws IOException {
    StreamingLeafManifestWriter writer =
        new StreamingLeafManifestWriter(writerFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY);
    writer.promoteCurrentToRoot(SNAPSHOT_ID, COMMIT_SEQ_NUMBER, 0L).close();
    assertThatThrownBy(() -> writer.promoteCurrentToRoot(SNAPSHOT_ID, COMMIT_SEQ_NUMBER, 0L))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("promoted");
  }

  @Test
  public void testConstructorRejectsInvalidArgs() {
    assertThatThrownBy(
            () -> new StreamingLeafManifestWriter(null, TARGET_BYTES, AVG_BYTES_PER_ENTRY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("writer supplier");
    assertThatThrownBy(
            () -> new StreamingLeafManifestWriter(writerFactory(), 0L, AVG_BYTES_PER_ENTRY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("targetSizeBytes must be positive");
    assertThatThrownBy(() -> new StreamingLeafManifestWriter(writerFactory(), TARGET_BYTES, 0L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("avgBytesPerEntry must be positive");
  }
}
