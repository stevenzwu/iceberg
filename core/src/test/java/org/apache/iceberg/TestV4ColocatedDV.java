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
import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests for Phase 6: colocated DV writes on v4 tables.
 *
 * <p>Most cases append a single small file, so the DV host stays a <em>root direct row</em> and the
 * DV collapses onto the promoted root — surfaced through the virtual manifest whose path is the
 * root itself. {@link #testAddDVToLeafResidentFile} instead uses a small target so the host spills
 * into a real on-disk leaf, exercising the separate leaf-rewrite path.
 *
 * <ul>
 *   <li>Adding a DV to an existing data file produces a REPLACED/MODIFIED pair and no separate
 *       position-delete manifest.
 *   <li>Replacing an existing DV produces a REPLACED/MODIFIED pair again.
 *   <li>A data file born with a DV in the same commit produces a single ADDED entry with the DV
 *       embedded.
 *   <li>The rewritten {@link ManifestFile} stats remain correct (MODIFIED entries fold into
 *       existing counts so that {@link ManifestFilterManager} pruning works).
 * </ul>
 */
public class TestV4ColocatedDV {

  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(100)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(5)
          .build();

  @TempDir File tableDir;

  private TestTables.TestTable table;

  @BeforeEach
  public void before() {
    table = TestTables.create(tableDir, tableDir.getName(), SCHEMA, SPEC, SortOrder.unsorted(), 4);
  }

  // ---- helpers ----------------------------------------------------------------

  /**
   * Reads raw {@link TrackedFile} rows from a v4 manifest file — a real on-disk leaf, or the
   * promoted root itself when the DV host stays a direct row (path equal to the snapshot file).
   */
  private List<TrackedFile> readManifestRows(ManifestFile manifest) throws IOException {
    try (V4ManifestEntryProjector projector =
        new V4ManifestEntryProjector(
            table.io().newInputFile(manifest.path()),
            ManifestContent.DATA,
            manifest.partitionSpecId(),
            table.ops().current().specsById(),
            InheritableMetadataFactory.empty())) {
      return Lists.newArrayList(projector.rawRows());
    }
  }

  /** Reads a v4 manifest via the standard {@link ManifestFiles} API (EXISTING/ADDED/DELETED). */
  private List<ManifestEntry<DataFile>> readManifestEntries(ManifestFile manifest)
      throws IOException {
    List<ManifestEntry<DataFile>> result = Lists.newArrayList();
    try (CloseableIterable<ManifestEntry<DataFile>> iter =
        ManifestFiles.read(manifest, table.io(), table.ops().current().specsById()).entries()) {
      for (ManifestEntry<DataFile> entry : iter) {
        result.add(entry.copy());
      }
    }
    return result;
  }

  // ---- tests ------------------------------------------------------------------

  /**
   * Adding a DV to an existing data file via {@code RowDelta} on a v4 table:
   *
   * <ul>
   *   <li>The promoted root is rewritten with a REPLACED/MODIFIED pair for FILE_A (the host stays a
   *       root direct row at the default target).
   *   <li>No separate position-delete manifest is produced.
   *   <li>The manifest reports {@code hasExistingFiles()} = true.
   *   <li>The manifest reports {@code replacedFilesCount()} = 1.
   * </ul>
   */
  @Test
  public void testAddDVToExistingFile() throws IOException {
    // snapshot 1: append FILE_A
    table.newAppend().appendFile(FILE_A).commit();

    // snapshot 2: add a DV for FILE_A
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    // no separate position-delete manifests in v4 with colocated DVs
    assertThat(snap2.deleteManifests(table.io()))
        .as("v4 colocated DV must not produce a position-delete manifest")
        .isEmpty();

    // exactly one data manifest — the virtual manifest over the promoted root's direct rows
    List<ManifestFile> dataManifests = snap2.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    ManifestFile rootManifest = dataManifests.get(0);
    assertThat(rootManifest.path())
        .as("host stays a root direct row at the default target, so the DV collapses onto the root")
        .isEqualTo(snap2.snapshotFileLocation());

    // manifest stats: MODIFIED folds into existing counts
    assertThat(rootManifest.hasExistingFiles())
        .as("rewritten manifest must report hasExistingFiles()=true for MODIFIED entry")
        .isTrue();

    // raw rows: REPLACED + MODIFIED pair for FILE_A
    List<TrackedFile> rows = readManifestRows(rootManifest);
    assertThat(rows).as("REPLACED + MODIFIED = 2 rows").hasSize(2);

    TrackedFile replacedRow = null;
    TrackedFile modifiedRow = null;
    for (TrackedFile row : rows) {
      if (row.tracking().status() == EntryStatus.REPLACED) {
        replacedRow = row;
      } else if (row.tracking().status() == EntryStatus.MODIFIED) {
        modifiedRow = row;
      }
    }

    assertThat(replacedRow).as("must have a REPLACED row for FILE_A").isNotNull();
    assertThat(modifiedRow).as("must have a MODIFIED row for FILE_A").isNotNull();

    // REPLACED row carries the prior DV (null here because FILE_A had no DV before snap2)
    assertThat(replacedRow.deletionVector())
        .as("REPLACED row must carry the prior DV (null when prior state had no DV)")
        .isNull();

    // MODIFIED row must carry the DV
    assertThat(modifiedRow.deletionVector())
        .as("MODIFIED row must carry an embedded deletion_vector")
        .isNotNull();
    assertThat(modifiedRow.deletionVector().location())
        .as("embedded DV location must match the committed DV")
        .isEqualTo(dv.location());

    // both rows reference FILE_A
    assertThat(replacedRow.location()).isEqualTo(FILE_A.location());
    assertThat(modifiedRow.location()).isEqualTo(FILE_A.location());
  }

  /**
   * Replacing an existing DV on an existing data file:
   *
   * <ul>
   *   <li>First commit attaches DV1 to FILE_A (REPLACED/MODIFIED).
   *   <li>Second commit attaches DV2 to FILE_A (REPLACED/MODIFIED again).
   *   <li>After the second commit the live MODIFIED row carries DV2.
   * </ul>
   */
  @Test
  public void testReplaceExistingDV() throws IOException {
    // snapshot 1: FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // snapshot 2: add DV1
    DeleteFile dv1 = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();
    Snapshot snap2 = table.currentSnapshot();

    // snapshot 3: replace DV1 with DV2 — scope validation from snap2 so the validator only looks
    // at concurrent commits between snap2 and the new parent (none here), and does not flag DV1
    // (already in the parent chain) as a conflict.
    DeleteFile dv2 = FileGenerationUtil.generateDV(table, FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1)
        .addDeletes(dv2)
        .validateFromSnapshot(snap2.snapshotId())
        .commit();
    Snapshot snap3 = table.currentSnapshot();

    assertThat(snap3.deleteManifests(table.io()))
        .as("still no position-delete manifests after DV replacement")
        .isEmpty();

    List<ManifestFile> dataManifests = snap3.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    List<TrackedFile> rows = readManifestRows(dataManifests.get(0));

    // find the live row (MODIFIED)
    TrackedFile liveRow = null;
    for (TrackedFile row : rows) {
      EntryStatus status = row.tracking().status();
      if (status == EntryStatus.MODIFIED || status == EntryStatus.ADDED) {
        liveRow = row;
      }
    }
    assertThat(liveRow).as("must have a live row for FILE_A").isNotNull();
    assertThat(liveRow.deletionVector()).as("live row must carry a DV").isNotNull();
    assertThat(liveRow.deletionVector().location())
        .as("live row must carry DV2, not DV1")
        .isEqualTo(dv2.location());

    // stale REPLACED + MODIFIED rows from snapshot 2 (carrying DV1) must NOT survive into the
    // child's promoted root. It should have exactly one REPLACED + one MODIFIED for
    // FILE_A — the rows produced by snapshot 3.
    TrackedFile replacedRow = null;
    TrackedFile modifiedRow = null;
    for (TrackedFile row : rows) {
      if (!row.location().equals(FILE_A.location().toString())) {
        continue;
      }
      if (row.tracking().status() == EntryStatus.REPLACED) {
        assertThat(replacedRow).as("exactly one REPLACED row for FILE_A").isNull();
        replacedRow = row;
      } else if (row.tracking().status() == EntryStatus.MODIFIED) {
        assertThat(modifiedRow).as("exactly one MODIFIED row for FILE_A").isNull();
        modifiedRow = row;
      }
    }
    assertThat(replacedRow).as("must have a REPLACED row for FILE_A").isNotNull();
    assertThat(modifiedRow).as("must have a MODIFIED row for FILE_A").isNotNull();

    // REPLACED row snapshot_id records the commit performing the replacement (snap3).
    assertThat(replacedRow.tracking().snapshotId())
        .as("REPLACED row must record the snapshot performing the replacement")
        .isEqualTo(snap3.snapshotId());

    // MODIFIED row snapshot_id preserves FILE_A's original ADD snapshot (snap1) so consumers can
    // trace when the base file was added; dv_snapshot_id advances to snap3 (the commit that
    // updated the DV).
    assertThat(modifiedRow.tracking().snapshotId())
        .as("MODIFIED row must preserve FILE_A's original ADD snapshot")
        .isEqualTo(snap1.snapshotId());
    assertThat(modifiedRow.tracking().dvSnapshotId())
        .as("MODIFIED row dv_snapshot_id must record the commit that updated the DV")
        .isEqualTo(snap3.snapshotId());
  }

  /**
   * A data file born with a DV in the same commit emits a single ADDED entry with the DV embedded —
   * no REPLACED/MODIFIED pair, no separate delete manifest.
   */
  @Test
  public void testBornWithDV() throws IOException {
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addRows(FILE_A).addDeletes(dv).commit();
    Snapshot snap = table.currentSnapshot();

    // no position-delete manifests
    assertThat(snap.deleteManifests(table.io()))
        .as("born-with-DV must not produce a separate position-delete manifest")
        .isEmpty();

    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    List<TrackedFile> rows = readManifestRows(dataManifests.get(0));
    assertThat(rows).as("born-with-DV must produce exactly one row").hasSize(1);

    TrackedFile row = rows.get(0);
    assertThat(row.tracking().status())
        .as("born-with-DV entry must be ADDED")
        .isEqualTo(EntryStatus.ADDED);
    assertThat(row.location()).isEqualTo(FILE_A.location());
    assertThat(row.deletionVector())
        .as("born-with-DV ADDED row must carry an embedded deletion_vector")
        .isNotNull();
    assertThat(row.deletionVector().location())
        .as("embedded DV location must match the committed DV")
        .isEqualTo(dv.location());
  }

  /**
   * Single-call born-with-DV via {@link RowDelta#addRows(DataFile, DeleteFile)} produces the same
   * ADDED-with-embedded-DV root direct row as the two-call chained pattern above.
   */
  @Test
  public void testAddRowsBornWithDVSingleCall() throws IOException {
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addRows(FILE_A, dv).commit();
    Snapshot snap = table.currentSnapshot();

    assertThat(snap.deleteManifests(table.io()))
        .as("single-call born-with-DV must not produce a separate position-delete manifest")
        .isEmpty();

    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    List<TrackedFile> rows = readManifestRows(dataManifests.get(0));
    assertThat(rows).hasSize(1);
    TrackedFile row = rows.get(0);
    assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(row.location()).isEqualTo(FILE_A.location());
    assertThat(row.deletionVector()).isNotNull();
    assertThat(row.deletionVector().location()).isEqualTo(dv.location());
  }

  /**
   * Single-call born-with-DV via {@link AppendFiles#appendFile(DataFile, DeleteFile)} produces the
   * same ADDED-with-embedded-DV root direct row as the RowDelta single-call and chained variants.
   */
  @Test
  public void testAppendFileBornWithDVSingleCall() throws IOException {
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newAppend().appendFile(FILE_A, dv).commit();
    Snapshot snap = table.currentSnapshot();

    assertThat(snap.deleteManifests(table.io()))
        .as("FastAppend born-with-DV must not produce a separate position-delete manifest")
        .isEmpty();

    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    List<TrackedFile> rows = readManifestRows(dataManifests.get(0));
    assertThat(rows).hasSize(1);
    TrackedFile row = rows.get(0);
    assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED);
    assertThat(row.location()).isEqualTo(FILE_A.location());
    assertThat(row.deletionVector()).isNotNull();
    assertThat(row.deletionVector().location()).isEqualTo(dv.location());
  }

  /** {@link AppendFiles#appendFile(DataFile, DeleteFile)} rejects a non-puffin DeleteFile. */
  @Test
  public void testAppendFileBornWithDVRejectsNonDV() {
    DeleteFile notADV =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/position-delete.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    assertThatThrownBy(() -> table.newAppend().appendFile(FILE_A, notADV))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be a puffin position-delete file");
  }

  /**
   * {@link AppendFiles#appendFile(DataFile, DeleteFile)} rejects a DV whose {@code
   * referencedDataFile} does not match the accompanying data file.
   */
  @Test
  public void testAppendFileBornWithDVRejectsMismatchedReferencedDataFile() {
    DataFile fileB =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(100)
            .withPartitionPath("data_bucket=1")
            .withRecordCount(5)
            .build();
    DeleteFile dvForB = FileGenerationUtil.generateDV(table, fileB);

    assertThatThrownBy(() -> table.newAppend().appendFile(FILE_A, dvForB))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must reference data file");
  }

  /** {@link RowDelta#addRows(DataFile, DeleteFile)} enforces the same DV / reference invariants. */
  @Test
  public void testAddRowsBornWithDVRejectsMismatchedReferencedDataFile() {
    DataFile fileB =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(100)
            .withPartitionPath("data_bucket=1")
            .withRecordCount(5)
            .build();
    DeleteFile dvForB = FileGenerationUtil.generateDV(table, fileB);

    assertThatThrownBy(() -> table.newRowDelta().addRows(FILE_A, dvForB))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must reference data file");
  }

  /**
   * After a v4 DV commit the legacy {@link ManifestFiles} reader maps REPLACED → DELETED (non-live)
   * and MODIFIED → EXISTING (live) so that {@link ManifestEntry#isLive()} agrees with the v4
   * tracking semantics: a REPLACED row is the prior state being superseded.
   */
  @Test
  public void testLegacyReaderSeesExistingForModified() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    List<ManifestFile> dataManifests = snap2.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    List<ManifestEntry<DataFile>> entries = readManifestEntries(dataManifests.get(0));

    // The REPLACED/MODIFIED pair surfaces to the legacy reader as DELETED (REPLACED) + EXISTING
    // (MODIFIED). isLive() returns false for the REPLACED row and true for the MODIFIED row.
    List<ManifestEntry<DataFile>> existingEntries = Lists.newArrayList();
    List<ManifestEntry<DataFile>> deletedEntries = Lists.newArrayList();
    for (ManifestEntry<DataFile> entry : entries) {
      if (entry.status() == ManifestEntry.Status.EXISTING) {
        existingEntries.add(entry);
      } else if (entry.status() == ManifestEntry.Status.DELETED) {
        deletedEntries.add(entry);
      }
    }

    assertThat(existingEntries).as("legacy reader must see MODIFIED as EXISTING (live)").hasSize(1);
    assertThat(deletedEntries)
        .as("legacy reader must see REPLACED as DELETED (non-live)")
        .hasSize(1);
    assertThat(existingEntries.get(0).file().location()).isEqualTo(FILE_A.location());
    assertThat(deletedEntries.get(0).file().location()).isEqualTo(FILE_A.location());
  }

  /**
   * Manifest stats after a DV rewrite are correct: MODIFIED entries fold into existingFilesCount so
   * that manifest-level pruning ({@link ManifestFilterManager}) does not skip live manifests. The
   * host stays a root direct row here, so the stats come from the promoted root.
   */
  @Test
  public void testManifestFileStatsAfterDVRewrite() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    List<ManifestFile> dataManifests = snap2.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);

    ManifestFile rootManifest = dataManifests.get(0);
    assertThat(rootManifest.path())
        .as("host stays a root direct row at the default target")
        .isEqualTo(snap2.snapshotFileLocation());

    // existingFilesCount must be 1 (the MODIFIED entry, folded in via toManifestFile())
    assertThat(rootManifest.existingFilesCount())
        .as("MODIFIED entry must be counted as existingFilesCount")
        .isEqualTo(1);

    // addedFilesCount must be 0 (no ADDED entries in the rewrite)
    assertThat(rootManifest.addedFilesCount())
        .as("no ADDED entries in a DV-rewrite manifest")
        .isEqualTo(0);

    // hasExistingFiles() must be true so filter manager does not prune this manifest
    assertThat(rootManifest.hasExistingFiles()).isTrue();

    // Verify the raw rows have a REPLACED + MODIFIED pair (replacedFilesCount is not round-tripped
    // through the root manifest in Phase 6 — that is a Phase 7 RootManifestReader concern).
    List<TrackedFile> rows = readManifestRows(rootManifest);
    long replacedCount =
        rows.stream().filter(r -> r.tracking().status() == EntryStatus.REPLACED).count();
    assertThat(replacedCount).as("raw rows must have exactly 1 REPLACED row").isEqualTo(1L);

    // ManifestFile-level REPLACED counts are populated by ManifestWriter.toManifestFile() and
    // persisted via the v4 root manifest's manifest_info struct. The read-side ManifestFile object
    // exposes them via the v4 interface default methods.
    assertThat(rootManifest.replacedFilesCount())
        .as("ManifestFile must report 1 REPLACED entry after the DV rewrite")
        .isEqualTo(1);
    assertThat(rootManifest.replacedRowsCount())
        .as("ManifestFile must report REPLACED rows = FILE_A.recordCount()")
        .isEqualTo(FILE_A.recordCount());
  }

  /**
   * Adding a DV to a data file that lives in an on-disk leaf rather than a root direct row. A small
   * target makes the initial append spill the host into a real leaf; the follow-up {@code RowDelta}
   * then routes through {@link MergingSnapshotProducer#rewriteLeafManifestsWithDVs}, rewriting that
   * leaf with a REPLACED/MODIFIED pair instead of collapsing the DV onto a root direct row. The
   * rewritten leaf is a real on-disk manifest (its path is not the snapshot file) and no separate
   * position-delete manifest is produced. Complements {@link #testAddDVToExistingFile}, whose host
   * stays a root direct row.
   */
  @Test
  public void testAddDVToLeafResidentFile() throws IOException {
    // 200-byte seed against a 1000-byte target → the append rolls a leaf every 5 rows.
    table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1000").commit();

    // Snap 1: five files spill into a single on-disk leaf with no direct-row remainder. FILE_A is
    // one of them, so it is leaf-resident rather than a root direct row.
    List<DataFile> files = Lists.newArrayList(FILE_A);
    for (int bucket = 1; bucket <= 4; bucket++) {
      files.add(
          DataFiles.builder(SPEC)
              .withPath("/path/to/data-leaf-" + bucket + ".parquet")
              .withFileSizeInBytes(100)
              .withPartitionPath("data_bucket=" + bucket)
              .withRecordCount(5)
              .build());
    }

    AppendFiles append = table.newAppend();
    for (DataFile file : files) {
      append.appendFile(file);
    }

    append.commit();
    Snapshot snap1 = table.currentSnapshot();

    List<ManifestFile> parentLeaves = Lists.newArrayList();
    for (ManifestFile manifest : snap1.dataManifests(table.io())) {
      if (!manifest.path().equals(snap1.snapshotFileLocation())) {
        parentLeaves.add(manifest);
      }
    }

    assertThat(parentLeaves).as("FILE_A must land in a real on-disk leaf").hasSize(1);
    assertThat(readManifestRows(parentLeaves.get(0)))
        .as("FILE_A must be leaf-resident, not a root direct row")
        .extracting(TrackedFile::location)
        .contains(FILE_A.location());

    // Snap 2: add a DV for the leaf-resident FILE_A.
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    assertThat(snap2.deleteManifests(table.io()))
        .as("colocated DV on a leaf-resident file must not produce a position-delete manifest")
        .isEmpty();

    // The DV rewrite goes through the leaf path: a real on-disk leaf (path != snapshot file) is
    // rewritten, rather than the DV collapsing onto a root direct row.
    List<ManifestFile> childLeaves = Lists.newArrayList();
    for (ManifestFile manifest : snap2.dataManifests(table.io())) {
      if (!manifest.path().equals(snap2.snapshotFileLocation())) {
        childLeaves.add(manifest);
      }
    }

    assertThat(childLeaves)
        .as("DV rewrite must produce a real on-disk leaf, not collapse onto a root direct row")
        .hasSize(1);

    List<TrackedFile> rows = readManifestRows(childLeaves.get(0));
    TrackedFile replacedRow = null;
    TrackedFile modifiedRow = null;
    for (TrackedFile row : rows) {
      if (!row.location().equals(FILE_A.location())) {
        continue;
      }

      if (row.tracking().status() == EntryStatus.REPLACED) {
        replacedRow = row;
      } else if (row.tracking().status() == EntryStatus.MODIFIED) {
        modifiedRow = row;
      }
    }

    assertThat(replacedRow).as("leaf rewrite must emit a REPLACED row for FILE_A").isNotNull();
    assertThat(modifiedRow).as("leaf rewrite must emit a MODIFIED row for FILE_A").isNotNull();
    assertThat(modifiedRow.deletionVector())
        .as("MODIFIED row must carry the embedded DV")
        .isNotNull();
    assertThat(modifiedRow.deletionVector().location()).isEqualTo(dv.location());
    assertThat(replacedRow.deletionVector())
        .as("REPLACED row carries the prior DV — null here because FILE_A had none")
        .isNull();

    // The four untouched files are carried forward as EXISTING, preserving their original append
    // snapshot id. The DV rewrite must not re-stamp pre-existing files under the committing
    // snapshot (which would wrongly attribute them to the DV commit).
    assertThat(rows)
        .filteredOn(r -> !r.location().equals(FILE_A.location()))
        .as("untouched leaf files must survive the DV rewrite as EXISTING")
        .hasSize(4)
        .allSatisfy(
            r -> {
              assertThat(r.tracking().status()).isEqualTo(EntryStatus.EXISTING);
              assertThat(r.tracking().snapshotId())
                  .as("survivor must keep its original append snapshot id, not the DV commit's")
                  .isEqualTo(snap1.snapshotId());
            });

    // End-to-end guard on the provenance fix: a DV-only commit must report no added data files.
    // FILE_A is MODIFIED and the four survivors are EXISTING; before the fix the survivors surfaced
    // here as ADDED under snap2.
    assertThat(snap2.addedDataFiles(table.io()))
        .as("DV rewrite on a leaf-resident file must not report added data files")
        .isEmpty();
  }

  /**
   * Two concurrent {@code RowDelta} commits that both add a DV for the same data file must
   * conflict. The first commit lands; the second commit's {@code
   * validateNoConflictingDeleteFiles()} (run as part of commit) must detect the colocated DV in the
   * concurrent data manifest and throw {@link ValidationException}.
   */
  @Test
  public void testConcurrentlyAddedColocatedDVsConflict() {
    // snapshot 1: append FILE_A
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot baseSnapshot = table.currentSnapshot();

    // prepare two RowDelta builders from the same starting snapshot, each adding a DV for FILE_A
    DeleteFile dv1 = FileGenerationUtil.generateDV(table, FILE_A);
    RowDelta rowDelta1 =
        table
            .newRowDelta()
            .addDeletes(dv1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    DeleteFile dv2 = FileGenerationUtil.generateDV(table, FILE_A);
    RowDelta rowDelta2 =
        table
            .newRowDelta()
            .addDeletes(dv2)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // commit rowDelta1 first — succeeds
    rowDelta1.commit();

    // rowDelta2 must fail with a concurrent-DV validation error: the v4 path walks the data
    // manifest from snap2 and sees the MODIFIED row for FILE_A carrying dv1.
    assertThatThrownBy(rowDelta2::commit)
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Found concurrently added DV for %s", FILE_A.location());
  }

  /**
   * Adding a DV to an existing data file must credit the snapshot summary with the same metrics as
   * a v3 standalone DV write: {@code added-dvs}, {@code added-delete-files}, {@code
   * added-position-deletes}, and {@code added-files-size}.
   */
  @Test
  public void testAddDVToExistingFilePopulatesSummary() {
    table.newAppend().appendFile(FILE_A).commit();
    Map<String, String> snap1Summary = table.currentSnapshot().summary();
    assertThat(snap1Summary)
        .as("baseline append must not credit DV-related counters")
        .doesNotContainKey(SnapshotSummary.ADDED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.ADDED_POS_DELETES_PROP);

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();

    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(dv.recordCount()))
        .containsEntry(
            SnapshotSummary.ADDED_FILE_SIZE_PROP, String.valueOf(dv.contentSizeInBytes()))
        .doesNotContainKey(SnapshotSummary.ADD_POS_DELETE_FILES_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_POS_DELETES_PROP);
  }

  /**
   * A born-with-DV commit (data file + DV in the same snapshot) must credit BOTH the added data
   * file and the embedded DV on the snapshot summary.
   */
  @Test
  public void testBornWithDVPopulatesSummary() {
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addRows(FILE_A).addDeletes(dv).commit();

    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary)
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, String.valueOf(FILE_A.recordCount()))
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(dv.recordCount()))
        .doesNotContainKey(SnapshotSummary.REMOVED_DVS_PROP);
  }

  /**
   * Replacing DV1 with DV2 in a follow-up commit must credit DV2 as added and DV1 as removed on
   * that commit's summary, mirroring the v3 standalone-DV behavior.
   */
  @Test
  public void testReplaceDVPopulatesSummary() {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv1 = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();
    Snapshot snap2 = table.currentSnapshot();
    Map<String, String> snap2Summary = snap2.summary();
    assertThat(snap2Summary)
        .as("snap2 must credit DV1 as added with no removals")
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(dv1.recordCount()))
        .doesNotContainKey(SnapshotSummary.REMOVED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_POS_DELETES_PROP);

    DeleteFile dv2 = FileGenerationUtil.generateDV(table, FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1)
        .addDeletes(dv2)
        .validateFromSnapshot(snap2.snapshotId())
        .commit();

    Map<String, String> snap3Summary = table.currentSnapshot().summary();
    assertThat(snap3Summary)
        .as("snap3 must credit DV2 as added and DV1 as removed")
        .containsEntry(SnapshotSummary.ADDED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_DVS_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.REMOVED_DELETE_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_POS_DELETES_PROP, String.valueOf(dv2.recordCount()))
        .containsEntry(SnapshotSummary.REMOVED_POS_DELETES_PROP, String.valueOf(dv1.recordCount()))
        .containsEntry(
            SnapshotSummary.ADDED_FILE_SIZE_PROP, String.valueOf(dv2.contentSizeInBytes()))
        .containsEntry(
            SnapshotSummary.REMOVED_FILE_SIZE_PROP, String.valueOf(dv1.contentSizeInBytes()));
  }

  /** Deleting a data file without any DV operation must not credit DV counters on the summary. */
  @Test
  public void testDataFileDeleteDoesNotPopulateDVSummary() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newDelete().deleteFile(FILE_A).commit();

    Map<String, String> summary = table.currentSnapshot().summary();
    assertThat(summary)
        .as("plain data-file delete must not credit DV counters")
        .doesNotContainKey(SnapshotSummary.ADDED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_DVS_PROP)
        .doesNotContainKey(SnapshotSummary.ADDED_POS_DELETES_PROP)
        .doesNotContainKey(SnapshotSummary.REMOVED_POS_DELETES_PROP);
  }

  // ---- SnapshotChanges / BaseSnapshot visibility for v4 colocated DVs --------

  /**
   * Adding a DV to an existing data file via a v4 RowDelta must surface through {@link
   * SnapshotChanges}:
   *
   * <ul>
   *   <li>{@code addedDataFiles()} is empty — the data file is still live, only its DV changed.
   *   <li>{@code addedDeleteFiles()} contains exactly the new DV.
   *   <li>{@code removedDeleteFiles()} is empty — no prior DV was superseded.
   * </ul>
   */
  @Test
  @Disabled(
      "v4 change detection is deferred: SnapshotChanges routes v4 leaves through the legacy adapter,"
          + " which collapses REPLACED to DELETED and so mis-reports DV-only rewrites as data-file"
          + " removals. Re-enable when v4-aware change detection joins REPLACED/MODIFIED pairs.")
  public void testSnapshotChangesAddDV() {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snap2).build();

    assertThat(changes.addedDataFiles())
        .as("DV-only commit must not report any added data files")
        .isEmpty();
    assertThat(changes.removedDataFiles())
        .as("DV-only commit must not report any removed data files")
        .isEmpty();
    assertThat(changes.addedDeleteFiles())
        .as("DV-only commit must report the new DV as an added delete file")
        .extracting(DeleteFile::location)
        .containsExactly(dv.location());
    assertThat(changes.removedDeleteFiles())
        .as("first-time DV add must not report any removed delete files")
        .isEmpty();
  }

  /**
   * Replacing an existing DV (DV1 → DV2) across two snapshots must surface DV2 as added and DV1 as
   * removed in the second snapshot's {@link SnapshotChanges}.
   */
  @Test
  @Disabled(
      "v4 change detection is deferred: SnapshotChanges routes v4 leaves through the legacy adapter,"
          + " which collapses REPLACED to DELETED and so mis-reports DV-only rewrites as data-file"
          + " removals. Re-enable when v4-aware change detection joins REPLACED/MODIFIED pairs.")
  public void testSnapshotChangesReplaceDV() {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv1 = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();
    Snapshot snap2 = table.currentSnapshot();

    DeleteFile dv2 = FileGenerationUtil.generateDV(table, FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1)
        .addDeletes(dv2)
        .validateFromSnapshot(snap2.snapshotId())
        .commit();
    Snapshot snap3 = table.currentSnapshot();

    SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snap3).build();

    assertThat(changes.addedDataFiles())
        .as("DV-only replace must not report any added data files")
        .isEmpty();
    assertThat(changes.removedDataFiles())
        .as("DV-only replace must not report any removed data files")
        .isEmpty();
    assertThat(changes.addedDeleteFiles())
        .as("DV-only replace must report DV2 as the new delete file")
        .extracting(DeleteFile::location)
        .containsExactly(dv2.location());
    assertThat(changes.removedDeleteFiles())
        .as("DV-only replace must report DV1 as a removed delete file")
        .extracting(DeleteFile::location)
        .containsExactly(dv1.location());
  }

  /**
   * A born-with-DV commit (data file + DV in the same snapshot) must surface BOTH the new data file
   * and the new DV through {@link SnapshotChanges}.
   */
  @Test
  public void testSnapshotChangesBornWithDV() {
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addRows(FILE_A).addDeletes(dv).commit();
    Snapshot snap = table.currentSnapshot();

    SnapshotChanges changes = SnapshotChanges.builderFor(table).snapshot(snap).build();

    assertThat(changes.addedDataFiles())
        .as("born-with-DV must report the new data file")
        .extracting(DataFile::location)
        .containsExactly(FILE_A.location());
    assertThat(changes.removedDataFiles()).isEmpty();
    assertThat(changes.addedDeleteFiles())
        .as("born-with-DV must report the embedded DV as an added delete file")
        .extracting(DeleteFile::location)
        .containsExactly(dv.location());
    assertThat(changes.removedDeleteFiles()).isEmpty();
  }

  /**
   * The deprecated {@code snapshot.addedDataFiles(io)} / {@code snapshot.removedDataFiles(io)}
   * accessors must not misclassify v4 REPLACED/MODIFIED rows as data-file changes. Before this fix,
   * a DV-only update threw an {@code IllegalStateException} from {@code
   * BaseSnapshot.cacheDataFileChanges} because the legacy reader projected REPLACED → DELETED which
   * the switch did not expect.
   */
  @Test
  public void testSnapshotAddedDataFilesIgnoresDVRewrite() {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    assertThat(snap2.addedDataFiles(table.io()))
        .as("DV-only commit must not surface any added data files via deprecated API")
        .isEmpty();
    assertThat(snap2.removedDataFiles(table.io()))
        .as("DV-only commit must not surface any removed data files via deprecated API")
        .isEmpty();
  }

  /**
   * The deprecated {@code snapshot.addedDeleteFiles(io)} accessor must surface v4 colocated DVs as
   * added delete files alongside legacy delete-manifest entries.
   */
  @Test
  public void testSnapshotAddedDeleteFilesIncludesColocatedDV() {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    assertThat(snap2.addedDeleteFiles(table.io()))
        .as("v4 colocated DV must surface as an added delete file via deprecated API")
        .extracting(DeleteFile::location)
        .containsExactly(dv.location());
    assertThat(snap2.removedDeleteFiles(table.io()))
        .as("first-time DV add must not surface any removed delete files via deprecated API")
        .isEmpty();
  }

  // Phase 12 API overloads (appendFile/addRows/updateDV with DeletionVector arg) are not part of
  // Phase 6; those tests are deferred until the convenience wrappers land on top of the DV-collapse
  // machinery.
}
