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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link V4CommitAccumulator}: covers pool routing via
 * {@link V4RootManifestAssembler#classify}, live-pool streaming through {@link V4StreamingWriter},
 * buffered retirement pools via {@link V4WritePool}, and the promote-to-root close protocol.
 *
 * <p>Row-level content verification (which specific rows end up as direct rows in the promoted
 * root vs which become leaf-manifest-entries) is covered by the end-to-end
 * {@code TestV4SnapshotProducer} injection tests — those read the resulting root Parquet file.
 * Here we focus on structural behavior: leaf counts, factory invocation, promotion invariants.
 */
public class TestV4CommitAccumulator {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();
  private static final Types.StructType UNION_PARTITION_TYPE = SPEC.partitionType();
  private static final long SNAPSHOT_ID = 42L;
  private static final long SEQUENCE_NUMBER = 1L;
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

  private TrackedFile dataRow(int i, EntryStatus status) {
    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-" + status + "-" + i + ".parquet")
            .withFileSizeInBytes(1024L)
            .withPartitionPath("data_bucket=" + (i % 16))
            .withRecordCount(10L)
            .build();
    Tracking tracking =
        new TrackingStruct(status, SNAPSHOT_ID, 3L, 3L, null, null, null, null);
    return TrackedFileAdapters.forDataFile(
            TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
            SCHEMA,
            MetricsConfig.from(ImmutableMap.of(), SCHEMA, SortOrder.unsorted()),
            UNION_PARTITION_TYPE)
        .wrap(dataFile, tracking);
  }

  private TrackedFileWriter.RootState refState() {
    return TrackedFileWriter.refOnlyState(SNAPSHOT_ID, SEQUENCE_NUMBER, 0L);
  }

  @Test
  public void testAllPoolsUnderTargetInline() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    // 2 rows per pool × 100 = 200 < 500 → no pool spills; live pool's single writer becomes root.
    for (int i = 0; i < 2; i++) {
      acc.add(dataRow(i, EntryStatus.ADDED), true);
      acc.add(dataRow(i, EntryStatus.DELETED), false);
      acc.add(dataRow(i, EntryStatus.REPLACED), false);
    }
    ManifestFile root = acc.close(refState());

    assertThat(root).isNotNull();
    assertThat(acc.leafManifests()).as("no rolled/spilled leaves").isEmpty();
    // Exactly one leaf writer opened — the live pool's promoted-root file.
    assertThat(leafCounter.get()).isEqualTo(1);
  }

  @Test
  public void testLivePoolStreamsAndRolls() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    // 6 live rows × 100 = 600 projected → rolls at 5 (1 leaf), 1 row remains in the next writer,
    // which the promote-to-root call promotes.
    for (int i = 0; i < 6; i++) {
      acc.add(dataRow(i, EntryStatus.ADDED), true);
    }
    ManifestFile root = acc.close(refState());

    assertThat(root).isNotNull();
    assertThat(acc.leafManifests()).as("one rolled leaf").hasSize(1);
    // 1 rolled leaf + 1 promoted-root writer.
    assertThat(leafCounter.get()).isEqualTo(2);
  }

  @Test
  public void testRetirementPoolsSpillIntoLeafRefs() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    // Each retirement pool: 6 rows × 100 = 600 → spills 1 leaf + 1 tail per pool.
    for (int i = 0; i < 6; i++) {
      acc.add(dataRow(i, EntryStatus.DELETED), false);
      acc.add(dataRow(i, EntryStatus.REPLACED), false);
    }
    ManifestFile root = acc.close(refState());

    assertThat(root).isNotNull();
    assertThat(acc.leafManifests()).as("2 retirement leaves").hasSize(2);
    // 2 retirement leaves + 1 promoted-root writer (live pool empty → new writer on promotion).
    assertThat(leafCounter.get()).isEqualTo(3);
  }

  @Test
  public void testEmptyAccumulatorPromotes() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    ManifestFile root = acc.close(refState());

    // No adds at all: promoteCurrentToRoot opens one writer just to hold zero rows + close as root.
    assertThat(root).isNotNull();
    assertThat(acc.leafManifests()).isEmpty();
    assertThat(leafCounter.get()).isEqualTo(1);
  }

  @Test
  public void testExternalLeafReferencesAreCarriedIntoRoot() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    // Add one live row so the live pool has content; then attach two external leaf refs. The
    // external refs land in the promoted root via addManifestEntry, distinct from
    // accumulator-produced leaves.
    acc.add(dataRow(0, EntryStatus.ADDED), true);
    ManifestFile external1 =
        new GenericManifestFile(
            "s3://bucket/external-1.parquet",
            1024L,
            SPEC.specId(),
            ManifestContent.DATA,
            /* seqNumber */ 1L,
            /* minSeqNumber */ 1L,
            SNAPSHOT_ID,
            /* partitions */ null,
            /* keyMetadata */ null,
            /* addedFilesCount */ 1,
            /* addedRowsCount */ 10L,
            /* existingFilesCount */ 0,
            /* existingRowsCount */ 0L,
            /* deletedFilesCount */ 0,
            /* deletedRowsCount */ 0L,
            /* firstRowId */ null,
            /* recordCount */ 1L,
            /* formatVersion */ 4,
            /* replacedFilesCount */ null,
            /* replacedRowsCount */ null);
    ManifestFile external2 =
        new GenericManifestFile(
            "s3://bucket/external-2.parquet",
            1024L,
            SPEC.specId(),
            ManifestContent.DATA,
            2L,
            2L,
            SNAPSHOT_ID + 1,
            null,
            null,
            0,
            0L,
            1,
            10L,
            0,
            0L,
            10L,
            1L,
            4,
            null,
            null);
    acc.addExternalLeafReference(external1, EntryStatus.ADDED);
    acc.addExternalLeafReference(external2, EntryStatus.EXISTING);

    ManifestFile root = acc.close(refState());

    assertThat(root).isNotNull();
    // Externals aren't in leafManifests(); they're refs, not accumulator-owned files.
    assertThat(acc.leafManifests()).isEmpty();
  }

  @Test
  public void testEqualityDeleteRowIsRejected() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    Tracking tracking =
        new TrackingStruct(EntryStatus.ADDED, SNAPSHOT_ID, 1L, 1L, null, null, null, null);
    TrackedFile eqDelete =
        new TrackedFileStruct(
            tracking,
            FileContent.EQUALITY_DELETES,
            4,
            "s3://bucket/eq-delete.parquet",
            FileFormat.PARQUET,
            10L,
            256L,
            SPEC.specId(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ImmutableList.of(1));

    assertThatThrownBy(() -> acc.add(eqDelete, true))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Delete pools");
  }

  @Test
  public void testCloseIsIdempotent() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    acc.add(dataRow(0, EntryStatus.ADDED), true);
    ManifestFile firstRoot = acc.close(refState());
    ManifestFile secondRoot = acc.close(refState());
    assertThat(secondRoot).isSameAs(firstRoot);
    assertThat(acc.promotedRoot()).isSameAs(firstRoot);
  }

  @Test
  public void testGettersRejectPreClose() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    assertThatThrownBy(acc::promotedRoot).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(acc::leafManifests).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testAddAfterCloseIsRejected() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    acc.close(refState());
    assertThatThrownBy(() -> acc.add(dataRow(0, EntryStatus.ADDED), true))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(
            () ->
                acc.addExternalLeafReference(
                    new GenericManifestFile("x", 1L, 0, ManifestContent.DATA, 1L, 1L, 1L, null,
                        null, 0, 0L, 0, 0L, 0, 0L, null, 0L, 4, null, null),
                    EntryStatus.ADDED))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testConstructorRejectsInvalidArgs() {
    assertThatThrownBy(
            () ->
                new V4CommitAccumulator(
                    null, TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new V4CommitAccumulator(
                    leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void testCloseRejectsNullRefState() {
    V4CommitAccumulator acc =
        new V4CommitAccumulator(
            leafFactory(), TARGET_BYTES, AVG_BYTES_PER_ENTRY, ImmutableList.of());
    assertThatThrownBy(() -> acc.close(null)).isInstanceOf(IllegalArgumentException.class);
  }
}
