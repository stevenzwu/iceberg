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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end smoke tests for {@link SnapshotProducer}'s v4 write path: asserts that committing to a
 * v4 table writes a root manifest ({@code .parquet}) instead of a manifest list ({@code .avro}),
 * and that manifest reference entries carry the correct {@link EntryStatus} and format_version.
 *
 * <p>Tests use a <em>partitioned</em> v4 table to avoid the Phase 2 known-issue with empty Parquet
 * row-groups in unpartitioned cases.
 */
public class TestV4SnapshotProducer {

  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  // Partitioned spec - bucket(data, 16).
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(1)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=1")
          .withRecordCount(1)
          .build();

  @TempDir File tableDir;

  private TestTables.TestTable table;

  @BeforeEach
  public void before() {
    // Use the temp dir name as the table name to guarantee uniqueness across test methods
    // (TestTables stores metadata in a static map keyed by table name).
    table = TestTables.create(tableDir, tableDir.getName(), SCHEMA, SPEC, SortOrder.unsorted(), 4);
  }

  // ---- helpers ----------------------------------------------------------------

  private List<TrackedFileStruct> readRootManifestRows(String rootManifestLocation)
      throws IOException {
    Schema contentEntrySchema =
        TrackedFile.schema(
            TrackedFileWriter.emptyPartitionPlaceholderIfNeeded(
                org.apache.iceberg.types.Types.StructType.of()),
            TrackedFileWriter.ROOT_CONTENT_STATS_TYPE);

    CloseableIterable<TrackedFileStruct> rows =
        InternalData.read(FileFormat.PARQUET, table.io().newInputFile(rootManifestLocation))
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .build();

    ImmutableList.Builder<TrackedFileStruct> result = ImmutableList.builder();
    try {
      for (TrackedFileStruct row : rows) {
        result.add(row);
      }
    } finally {
      rows.close();
    }
    return result.build();
  }

  private List<ManifestEntry<DataFile>> readLeafManifestEntries(ManifestFile manifest)
      throws IOException {
    return readLeafManifestEntries(manifest, table);
  }

  private List<ManifestEntry<DataFile>> readLeafManifestEntries(
      ManifestFile manifest, TestTables.TestTable fromTable) throws IOException {
    ImmutableList.Builder<ManifestEntry<DataFile>> result = ImmutableList.builder();
    try (CloseableIterable<ManifestEntry<DataFile>> entries =
        ManifestFiles.read(manifest, fromTable.io(), fromTable.ops().current().specsById())
            .entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        result.add(entry);
      }
    }
    return result.build();
  }

  // ---- tests ------------------------------------------------------------------

  /**
   * First append to a v4 table stays inline as a root direct row — one file well below the 8 MB
   * default target does not spill a leaf.
   *
   * <ul>
   *   <li>Snapshot has rootManifestLocation set (.parquet), manifestListLocation null.
   *   <li>Root manifest carries one DATA direct row with status=ADDED — no DATA_MANIFEST refs.
   *   <li>{@link Snapshot#dataManifests} surfaces exactly one virtual manifest pointing at the root
   *       manifest itself (Phase 4d).
   * </ul>
   */
  @Test
  public void testAppendV4() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot snap = table.currentSnapshot();
    assertThat(snap).isNotNull();

    // v4: root manifest not manifest list
    assertThat(snap.rootManifestLocation())
        .as("root manifest location must be set for v4")
        .isNotNull()
        .endsWith(".parquet");
    assertThat(snap.manifestListLocation())
        .as("manifest list location must be null for v4")
        .isNull();

    // Root manifest must carry exactly one DATA direct row (no on-disk leaf).
    List<TrackedFileStruct> rootRows = readRootManifestRows(snap.rootManifestLocation());
    assertThat(rootRows).hasSize(1);

    TrackedFileStruct rootEntry = rootRows.get(0);
    assertThat(rootEntry.contentType())
        .as("root entry must be a DATA direct row")
        .isEqualTo(FileContent.DATA);
    assertThat(rootEntry.location())
        .as("direct row location must be the data file itself")
        .isEqualTo(FILE_A.location());

    Tracking tracking = rootEntry.tracking();
    assertThat(tracking).isNotNull();
    assertThat(tracking.status())
        .as("newly-added direct row must be ADDED")
        .isEqualTo(EntryStatus.ADDED);

    // dataManifests() surfaces one virtual manifest whose path is the root manifest itself.
    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    assertThat(dataManifests.get(0).path())
        .as("virtual manifest must point at the promoted root itself")
        .isEqualTo(snap.rootManifestLocation());
  }

  /**
   * FastAppend routes new DataFiles into the accumulator input channel. Two entries against the
   * default 8 MB target project well below the spill threshold, so both stay inline as root direct
   * rows and no leaf manifest is written.
   */
  @Test
  public void testAppendV4AdaptiveTreeSmallWriteStaysInline() throws IOException {

    // Covers MergeAppend (returned by table.newAppend()) — FastAppend's flag-on branch shares the
    // same injection helper on SnapshotProducer, so both paths are exercised end-to-end.
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot snap = table.currentSnapshot();
    assertThat(snap).isNotNull();
    assertThat(snap.rootManifestLocation())
        .as("root manifest location must be set for v4")
        .isNotNull()
        .endsWith(".parquet");
    assertThat(snap.manifestListLocation()).isNull();

    // Phase 4d: direct rows are surfaced via a synthetic virtual manifest whose path is the root
    // manifest itself. No on-disk leaf manifest is written, so the only data manifest surfaced is
    // the virtual one over the promoted root's direct rows.
    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    assertThat(dataManifests.get(0).path())
        .as("virtual manifest must point at the promoted root itself")
        .isEqualTo(snap.rootManifestLocation());

    List<TrackedFileStruct> rootRows = readRootManifestRows(snap.rootManifestLocation());
    assertThat(rootRows).hasSize(2);
    assertThat(rootRows)
        .allSatisfy(
            row -> {
              assertThat(row.contentType()).isEqualTo(FileContent.DATA);
              assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED);
            });
    assertThat(rootRows)
        .extracting(TrackedFileStruct::location)
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
  }

  /**
   * Phase 4c: adaptive multi-commit carries parent's direct rows into the child's promoted root as
   * EXISTING entries. Without this, the second commit's promoted root would only contain the
   * newly-appended file (FILE_C), losing FILE_A and FILE_B that were direct rows in the parent's
   * promoted root.
   */
  @Test
  public void testAdaptiveMultiCommitCarriesParentDirectRows() throws IOException {

    // Snap 1: two files land inline as direct rows in a promoted root.
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    // Snap 2: one more file — the child's promoted root must include all three.
    DataFile fileC =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-c.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2")
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(fileC).commit();

    Snapshot snap = table.currentSnapshot();
    List<DataFile> directRows =
        RootManifestReader.readDirectDataRows(
            table.io().newInputFile(snap.rootManifestLocation()),
            table.ops().current().specsById());

    assertThat(directRows)
        .as("child's promoted root must carry parent's FILE_A/FILE_B alongside the new FILE_C")
        .extracting(DataFile::location)
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location(), fileC.location());
  }

  /**
   * Phase 4b: filter manager consumes parent's promoted-root direct rows. FILE_A lives inline as a
   * direct row (no leaf manifest), so {@code snapshot.dataManifests()} is empty; without Phase 4b,
   * a delete targeting FILE_A would fail with "Missing required files to delete". After 4b, FILE_A
   * lands as a DELETED retirement direct row and FILE_B is carried over as EXISTING.
   */
  @Test
  public void testDeleteFileInParentDirectRows() throws IOException {

    // Snap 1: two files land inline as direct rows; no leaf manifest is written.
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot parent = table.currentSnapshot();
    // Phase 4d: dataManifests exposes a virtual manifest over the root's direct rows, but no
    // real on-disk leaf manifest exists for this small-write parent.
    assertThat(parent.dataManifests(table.io()))
        .as("small-write parent has only a virtual manifest over the root's direct rows")
        .hasSize(1)
        .allSatisfy(m -> assertThat(m.path()).isEqualTo(parent.rootManifestLocation()));

    // Snap 2: delete FILE_A by file reference. Without Phase 4b this would throw
    // "Missing required files to delete" because FILE_A is not surfaced via parent.dataManifests().
    table.newDelete().deleteFile(FILE_A).commit();

    Snapshot child = table.currentSnapshot();
    List<TrackedFileStruct> rootRows = readRootManifestRows(child.rootManifestLocation());
    // Data-file direct rows only (exclude any DATA_MANIFEST entries).
    List<TrackedFileStruct> dataRows =
        rootRows.stream()
            .filter(r -> r.contentType() == FileContent.DATA)
            .collect(Collectors.toList());

    // FILE_A is retired: DELETED direct row. FILE_B survives: EXISTING carry-over.
    assertThat(dataRows)
        .as("both files present as direct rows in the child promoted root")
        .hasSize(2);
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.EXISTING)
        .extracting(TrackedFileStruct::location)
        .containsExactly(FILE_B.location());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.DELETED)
        .extracting(TrackedFileStruct::location)
        .containsExactly(FILE_A.location());
  }

  /**
   * Phase 4b: an overwrite that retires multiple files from parent's promoted-root direct rows (all
   * inline) alongside adding a new file. Each retired file lands as a DELETED direct row and the
   * survivor stays as EXISTING carry-over, alongside the new ADDED file.
   */
  @Test
  public void testOverwriteWithMultipleParentDirectRowRetirements() throws IOException {

    DataFile fileC =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-c.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2")
            .withRecordCount(1)
            .build();

    // Snap 1: FILE_A and FILE_B land as direct rows.
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // Snap 2: overwrite drops FILE_A and adds FILE_C. FILE_B remains.
    table.newOverwrite().deleteFile(FILE_A).addFile(fileC).commit();

    Snapshot child = table.currentSnapshot();
    List<TrackedFileStruct> rootRows = readRootManifestRows(child.rootManifestLocation());
    List<TrackedFileStruct> dataRows =
        rootRows.stream()
            .filter(r -> r.contentType() == FileContent.DATA)
            .collect(Collectors.toList());

    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.ADDED)
        .extracting(TrackedFileStruct::location)
        .containsExactly(fileC.location());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.EXISTING)
        .extracting(TrackedFileStruct::location)
        .containsExactly(FILE_B.location());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.DELETED)
        .extracting(TrackedFileStruct::location)
        .containsExactly(FILE_A.location());
  }

  /**
   * Phase 4e: when the caller opts into {@code failMissingDeletePaths} (via {@code
   * validateFilesExist()} on a delete, or {@code validateNoConflictingData()} on an overwrite), the
   * commit must succeed for a direct-row data file. The require-lookup in {@link
   * ManifestFilterManager#validateRequiredDeletes} needs to consult the parent's promoted-root
   * direct rows via {@code directDeletedFiles} in addition to the on-disk leaf manifests; without
   * that consultation, retiring a direct-row file throws "Missing required files to delete".
   */
  @Test
  public void testValidateFilesExistForDirectRowFile() throws IOException {
    // Snap 1: FILE_A and FILE_B land inline as direct rows (no leaf manifest).
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // Snap 2: delete FILE_A with validateFilesExist() — StreamingDelete.validate opts into
    // failMissingDeletePaths, so ManifestFilterManager.validateRequiredDeletes runs.
    table.newDelete().deleteFile(FILE_A).validateFilesExist().commit();

    Snapshot child = table.currentSnapshot();
    List<TrackedFileStruct> rootRows = readRootManifestRows(child.rootManifestLocation());
    List<TrackedFileStruct> dataRows =
        rootRows.stream()
            .filter(r -> r.contentType() == FileContent.DATA)
            .collect(Collectors.toList());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.DELETED)
        .extracting(TrackedFileStruct::location)
        .containsExactly(FILE_A.location());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.EXISTING)
        .extracting(TrackedFileStruct::location)
        .containsExactly(FILE_B.location());
  }

  /**
   * Phase 4a: {@link RootManifestReader#readDirectDataRows} extracts direct DATA rows from the
   * promoted root as {@link DataFile} views. Foundation for filter-manager and scan-planning
   * integration in follow-up slices.
   */
  @Test
  public void testReadDirectDataRowsFromPromotedRoot() throws IOException {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot snap = table.currentSnapshot();
    List<DataFile> directRows =
        RootManifestReader.readDirectDataRows(
            table.io().newInputFile(snap.rootManifestLocation()),
            table.ops().current().specsById());

    assertThat(directRows).hasSize(2);
    assertThat(directRows)
        .extracting(DataFile::location)
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
    // Partition is projected correctly — each direct row keeps its bucket assignment.
    assertThat(directRows).allSatisfy(f -> assertThat(f.specId()).isEqualTo(SPEC.specId()));
  }

  /**
   * Exercises the v4 adaptive-tree drain path by injecting synthetic {@link TrackedFile} rows into
   * {@link SnapshotProducer#addV4AdaptiveNewLiveDataRow} before commit. The 8 MB default target is
   * far above two 200-byte projected rows, so both stay as root direct rows and no leaf manifest is
   * written.
   */
  @Test
  public void testAdaptiveTreeDrainInjectedRowsAsDirectRows() throws IOException {

    AppendFiles append = table.newAppend();
    @SuppressWarnings("unchecked")
    SnapshotProducer<AppendFiles> producer = (SnapshotProducer<AppendFiles>) append;

    // Force snapshot-id assignment so injected rows carry the same id the commit will use.
    long snapId = producer.snapshotId();

    producer.addV4AdaptiveNewLiveDataRow(buildInjectedDataRow(FILE_A, snapId));
    producer.addV4AdaptiveNewLiveDataRow(buildInjectedDataRow(FILE_B, snapId));

    append.commit();

    Snapshot snap = table.currentSnapshot();
    assertThat(snap).isNotNull();
    assertThat(snap.rootManifestLocation()).isNotNull().endsWith(".parquet");
    // Phase 4d: the virtual manifest over the root's direct rows is the only data manifest.
    assertThat(snap.dataManifests(table.io()))
        .as("below-target buffer stays inline; only the virtual root-direct-rows manifest surfaces")
        .hasSize(1)
        .allSatisfy(m -> assertThat(m.path()).isEqualTo(snap.rootManifestLocation()));

    List<TrackedFileStruct> rootRows = readRootManifestRows(snap.rootManifestLocation());
    assertThat(rootRows).as("both injected rows must land as root direct rows").hasSize(2);
    assertThat(rootRows)
        .allSatisfy(
            row -> {
              assertThat(row.contentType()).isEqualTo(FileContent.DATA);
              assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED);
            });
    assertThat(rootRows)
        .extracting(TrackedFileStruct::location)
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
  }

  /**
   * With {@code commit.manifest.target-size-bytes=1}, every injected row projects over the target
   * and the accumulator spills; the resulting leaf-manifest-entries land in the root with status
   * ADDED. Verifies the spill branch of {@link SnapshotProducer#drainV4AdaptiveInputs}.
   */
  @Test
  public void testAdaptiveTreeDrainInjectedRowsSpillToLeaf() throws IOException {
    table
        .updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1")
        .commit();

    AppendFiles append = table.newAppend();
    @SuppressWarnings("unchecked")
    SnapshotProducer<AppendFiles> producer = (SnapshotProducer<AppendFiles>) append;

    long snapId = producer.snapshotId();
    producer.addV4AdaptiveNewLiveDataRow(buildInjectedDataRow(FILE_A, snapId));
    producer.addV4AdaptiveNewLiveDataRow(buildInjectedDataRow(FILE_B, snapId));

    append.commit();

    Snapshot snap = table.currentSnapshot();
    List<ManifestFile> leaves = snap.dataManifests(table.io());
    assertThat(leaves).as("with target-size=1 every row exceeds the spill threshold").isNotEmpty();

    List<TrackedFileStruct> rootRows = readRootManifestRows(snap.rootManifestLocation());
    assertThat(rootRows)
        .filteredOn(row -> row.contentType() == FileContent.DATA_MANIFEST)
        .as("spilled leaves surface as DATA_MANIFEST references in the root")
        .isNotEmpty()
        .allSatisfy(row -> assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED));
  }

  /**
   * End-to-end mixed adaptive commit: {@code target-size-bytes=1000} against the default
   * 200-byte-per-entry seed gives 5 entries per leaf. Seven appended files produce one real leaf
   * manifest (rows 1..5) and two root direct rows (rows 6, 7). Verifies the writer produces the
   * mixed structure and that {@link Snapshot#dataManifests} + {@code table.newScan().planFiles()}
   * surface files from both surfaces uniformly.
   */
  @Test
  public void testAdaptiveTreeMixedSpillAndDirectRows() throws IOException {
    table
        .updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1000")
        .commit();

    List<DataFile> files = Lists.newArrayList();
    for (int i = 0; i < 7; i++) {
      files.add(
          DataFiles.builder(SPEC)
              .withPath("/path/to/data-mix-" + i + ".parquet")
              .withFileSizeInBytes(10)
              .withPartitionPath("data_bucket=" + i)
              .withRecordCount(1)
              .build());
    }
    AppendFiles append = table.newAppend();
    for (DataFile f : files) {
      append.appendFile(f);
    }
    append.commit();

    Snapshot snap = table.currentSnapshot();
    assertThat(snap.rootManifestLocation()).isNotNull().endsWith(".parquet");

    // Root manifest carries 1 DATA_MANIFEST reference (the spilled leaf) + 2 DATA direct rows.
    List<TrackedFileStruct> rootRows = readRootManifestRows(snap.rootManifestLocation());
    List<TrackedFileStruct> dataManifestRefs =
        rootRows.stream()
            .filter(r -> r.contentType() == FileContent.DATA_MANIFEST)
            .collect(Collectors.toList());
    List<TrackedFileStruct> directDataRows =
        rootRows.stream()
            .filter(r -> r.contentType() == FileContent.DATA)
            .collect(Collectors.toList());
    assertThat(dataManifestRefs)
        .as("target/avg=5, 7 files → one spilled leaf")
        .hasSize(1)
        .allSatisfy(row -> assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED));
    assertThat(directDataRows)
        .as("sub-target tail of 2 files stays as root direct rows")
        .hasSize(2)
        .allSatisfy(row -> assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED));

    // dataManifests() surfaces both the real leaf and the virtual manifest over direct rows.
    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(2);
    ManifestFile virtual =
        dataManifests.stream()
            .filter(m -> m.path().equals(snap.rootManifestLocation()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("virtual manifest missing"));
    ManifestFile realLeaf =
        dataManifests.stream()
            .filter(m -> !m.path().equals(snap.rootManifestLocation()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("real leaf manifest missing"));
    assertThat(virtual.addedFilesCount()).isEqualTo(2);
    assertThat(virtual.content()).isEqualTo(ManifestContent.DATA);
    assertThat(realLeaf.addedFilesCount()).isEqualTo(5);
    assertThat(realLeaf.content()).isEqualTo(ManifestContent.DATA);

    // Scan planning surfaces all 7 files uniformly across both surfaces.
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      List<String> scannedPaths = Lists.newArrayList();
      for (FileScanTask t : tasks) {
        scannedPaths.add(t.file().location());
      }
      assertThat(scannedPaths)
          .containsExactlyInAnyOrderElementsOf(
              files.stream().map(DataFile::location).collect(Collectors.toList()));
    }
  }

  /**
   * Multi-commit story on top of a mixed adaptive parent: parent has both a real leaf and root
   * direct rows; child appends a new file and retires one direct-row file. Exercises Phase 4b
   * (filter manager retires a direct-row file) and Phase 4c (child carries parent's remaining
   * direct rows) simultaneously with a real leaf in play. Scan planning must surface every
   * still-live file from the mixed child snapshot.
   */
  @Test
  public void testAdaptiveTreeMultiCommitOnMixedParent() throws IOException {
    table
        .updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1000")
        .commit();

    // Snap 1: 7 files → one leaf of 5 (files 0..4) + two direct rows (files 5, 6).
    List<DataFile> parentFiles = Lists.newArrayList();
    for (int i = 0; i < 7; i++) {
      parentFiles.add(
          DataFiles.builder(SPEC)
              .withPath("/path/to/data-p-" + i + ".parquet")
              .withFileSizeInBytes(10)
              .withPartitionPath("data_bucket=" + i)
              .withRecordCount(1)
              .build());
    }
    AppendFiles parentAppend = table.newAppend();
    for (DataFile f : parentFiles) {
      parentAppend.appendFile(f);
    }
    parentAppend.commit();

    // Snap 2: append one new file and delete one of parent's direct rows.
    DataFile childNewFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-c-new.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=8")
            .withRecordCount(1)
            .build();
    DataFile parentDirectToDelete = parentFiles.get(5); // one of the two direct rows
    table.newOverwrite().deleteFile(parentDirectToDelete).addFile(childNewFile).commit();

    Snapshot child = table.currentSnapshot();

    // Scan planning surfaces the 6 surviving parent files + the newly-added child file.
    List<String> expected = Lists.newArrayList();
    for (DataFile f : parentFiles) {
      if (!f.location().equals(parentDirectToDelete.location())) {
        expected.add(f.location());
      }
    }
    expected.add(childNewFile.location());
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      List<String> scanned = Lists.newArrayList();
      for (FileScanTask t : tasks) {
        scanned.add(t.file().location());
      }
      assertThat(scanned).containsExactlyInAnyOrderElementsOf(expected);
    }

    // Sanity: the retired direct-row file surfaces as a DELETED direct row on the child's root,
    // confirming Phase 4b routed the retirement through filterV4AdaptiveParentDirectRows.
    List<TrackedFileStruct> childRootRows = readRootManifestRows(child.rootManifestLocation());
    assertThat(childRootRows)
        .filteredOn(
            r ->
                r.contentType() == FileContent.DATA
                    && r.tracking().status() == EntryStatus.DELETED)
        .extracting(TrackedFileStruct::location)
        .containsExactly(parentDirectToDelete.location());
  }

  /**
   * Phase 4d: RootManifestReader synthesizes a virtual {@link ManifestFile} covering direct DATA
   * rows in the promoted root. {@link Snapshot#dataManifests} surfaces this virtual manifest so
   * consumers see the inline data files (scan planning, addedDataFiles, etc.). Opening the virtual
   * manifest via {@link ManifestFiles#read} yields only the DATA rows — co-resident DATA_MANIFEST /
   * DELETE_MANIFEST rows are filtered out.
   */
  @Test
  public void testDataManifestsSurfacesDirectRowsAsVirtualManifest() throws IOException {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot snap = table.currentSnapshot();
    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    ManifestFile virtual = dataManifests.get(0);
    assertThat(virtual.path()).isEqualTo(snap.rootManifestLocation());
    assertThat(virtual.addedFilesCount()).isEqualTo(2);
    assertThat(virtual.snapshotId()).isEqualTo(snap.snapshotId());
    assertThat(virtual.content()).isEqualTo(ManifestContent.DATA);

    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(virtual, table.io(), table.ops().current().specsById())) {
      List<DataFile> files = Lists.newArrayList();
      for (ManifestEntry<DataFile> entry : reader.entries()) {
        files.add(entry.file());
      }
      assertThat(files)
          .extracting(DataFile::location)
          .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
    }
  }

  /**
   * Phase 4d: table scan planning sees direct DATA rows via the virtual manifest surfaced from
   * {@link Snapshot#dataManifests}. Without Phase 4d, {@code table.newScan().planFiles()} returns
   * empty for a table whose data lives entirely in direct rows.
   */
  @Test
  public void testTableScanSeesDirectRows() throws IOException {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      List<String> scannedPaths = Lists.newArrayList();
      for (FileScanTask t : tasks) {
        scannedPaths.add(t.file().location());
      }
      assertThat(scannedPaths).containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
    }
  }

  private TrackedFile buildInjectedDataRow(DataFile file, long snapId) {
    Types.StructType unionPartitionType = Partitioning.unionPartitionTypes(table.specs().values());
    MetricsConfig metricsConfig =
        MetricsConfig.from(table.properties(), SCHEMA, SortOrder.unsorted());
    Tracking tracking =
        new TrackingStruct(EntryStatus.ADDED, snapId, null, null, null, null, null, null);
    return TrackedFileAdapters.forDataFile(
            TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
            SCHEMA,
            metricsConfig,
            unionPartitionType)
        .wrap(file, tracking);
  }
}
