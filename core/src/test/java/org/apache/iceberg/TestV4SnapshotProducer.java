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
import java.util.stream.Collectors;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end smoke tests for {@link SnapshotProducer}'s v4 write path: asserts that committing to a
 * v4 table writes a root manifest ({@code .parquet}) instead of a manifest list ({@code .avro}),
 * and that manifest reference entries carry the correct {@link EntryStatus} and format_version.
 *
 * <p>Most tests use a partitioned v4 table; {@link #testUnpartitionedV4RoundTrips} covers the
 * unpartitioned case, where the empty partition type maps to {@code UnknownType} so no partition
 * column is written (read back as null).
 */
public class TestV4SnapshotProducer {

  private static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  // Partitioned spec - bucket(data, 16).
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  // Column-level stats for id=3:int, data=4:string. Each fixture picks distinct (id, data) bounds
  // so any test that later asserts a specific file's stats survived a rewrite is anchored to the
  // source file — sharing bounds would let a test pass by accidentally reading a different row's
  // bytes. {@link #buildInjectedDataRow} strips stats via {@code copyWithoutStats()} because the
  // raw-inject helper path does not tolerate DataFile-provided stats (unrelated to real append
  // flows).
  private static Metrics metrics(int id, String data) {
    return new Metrics(
        1L,
        ImmutableMap.of(3, 4L, 4, 8L),
        ImmutableMap.of(3, 1L, 4, 1L),
        ImmutableMap.of(3, 0L, 4, 0L),
        null,
        ImmutableMap.of(
            3, Conversions.toByteBuffer(Types.IntegerType.get(), id),
            4, Conversions.toByteBuffer(Types.StringType.get(), data)),
        ImmutableMap.of(
            3, Conversions.toByteBuffer(Types.IntegerType.get(), id),
            4, Conversions.toByteBuffer(Types.StringType.get(), data)));
  }

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withMetrics(metrics(1, "a"))
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=1")
          .withMetrics(metrics(2, "b"))
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

  private List<TrackedFile> readRootManifestRows(String rootManifestLocation) throws IOException {
    TableMetadata current = table.ops().current();
    try (V4ManifestEntryProjector projector =
        new V4ManifestEntryProjector(
            table.io().newInputFile(rootManifestLocation),
            ManifestContent.DATA,
            current.defaultSpecId(),
            current.specsById(),
            InheritableMetadataFactory.empty())) {
      return Lists.newArrayList(projector.rawRows());
    }
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

  /**
   * Data manifests that are real on-disk leaves — i.e. everything except the virtual manifest that
   * {@link RootManifestReader} synthesizes over the root's direct rows (whose path is the root
   * manifest itself). A non-empty result means the snapshot is a 2-level tree.
   */
  private List<ManifestFile> realLeaves(Snapshot snap) {
    return snap.dataManifests(table.io()).stream()
        .filter(m -> !m.path().equals(snap.snapshotFileLocation()))
        .collect(Collectors.toList());
  }

  private List<DataFile> newDataFiles(String prefix, int startBucket, int count) {
    List<DataFile> files = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      int bucket = startBucket + i;
      files.add(
          DataFiles.builder(SPEC)
              .withPath("/path/to/data-" + prefix + "-" + bucket + ".parquet")
              .withFileSizeInBytes(10)
              .withPartitionPath("data_bucket=" + bucket)
              .withMetrics(metrics(100 + bucket, prefix + "-" + bucket))
              .build());
    }

    return files;
  }

  private List<String> planFileLocations() throws IOException {
    List<String> paths = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        paths.add(task.file().location());
      }
    }

    return paths;
  }

  // ---- tests ------------------------------------------------------------------

  /**
   * First append to a v4 table stays inline as a root direct row — one file well below the 8 MB
   * default target does not spill a leaf.
   *
   * <ul>
   *   <li>Snapshot has snapshotFileLocation set (.parquet); manifestListLocation() throws.
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
    assertThat(snap.snapshotFileLocation())
        .as("root manifest location must be set for v4")
        .isNotNull()
        .endsWith(".parquet");
    assertThatThrownBy(snap::manifestListLocation)
        .as("manifestListLocation() must throw for v4 snapshots")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("has no manifest list");

    // Root manifest must carry exactly one DATA direct row (no on-disk leaf).
    List<TrackedFile> rootRows = readRootManifestRows(snap.snapshotFileLocation());
    assertThat(rootRows).hasSize(1);

    TrackedFile rootEntry = rootRows.get(0);
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
        .isEqualTo(snap.snapshotFileLocation());
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
    assertThat(snap.snapshotFileLocation())
        .as("root manifest location must be set for v4")
        .isNotNull()
        .endsWith(".parquet");
    assertThatThrownBy(snap::manifestListLocation)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("has no manifest list");

    // Phase 4d: direct rows are surfaced via a synthetic virtual manifest whose path is the root
    // manifest itself. No on-disk leaf manifest is written, so the only data manifest surfaced is
    // the virtual one over the promoted root's direct rows.
    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    assertThat(dataManifests.get(0).path())
        .as("virtual manifest must point at the promoted root itself")
        .isEqualTo(snap.snapshotFileLocation());

    List<TrackedFile> rootRows = readRootManifestRows(snap.snapshotFileLocation());
    assertThat(rootRows).hasSize(2);
    assertThat(rootRows)
        .allSatisfy(
            row -> {
              assertThat(row.contentType()).isEqualTo(FileContent.DATA);
              assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED);
            });
    assertThat(rootRows)
        .extracting(TrackedFile::location)
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
            .withMetrics(metrics(3, "c"))
            .build();
    table.newAppend().appendFile(fileC).commit();

    Snapshot snap = table.currentSnapshot();
    List<DataFile> directRows =
        RootManifestReader.readDirectDataRows(
            table.io().newInputFile(snap.snapshotFileLocation()),
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
        .allSatisfy(m -> assertThat(m.path()).isEqualTo(parent.snapshotFileLocation()));

    // Snap 2: delete FILE_A by file reference. Without Phase 4b this would throw
    // "Missing required files to delete" because FILE_A is not surfaced via parent.dataManifests().
    table.newDelete().deleteFile(FILE_A).commit();

    Snapshot child = table.currentSnapshot();
    List<TrackedFile> rootRows = readRootManifestRows(child.snapshotFileLocation());
    // Data-file direct rows only (exclude any DATA_MANIFEST entries).
    List<TrackedFile> dataRows =
        rootRows.stream()
            .filter(r -> r.contentType() == FileContent.DATA)
            .collect(Collectors.toList());

    // FILE_A is retired: DELETED direct row. FILE_B survives: EXISTING carry-over.
    assertThat(dataRows)
        .as("both files present as direct rows in the child promoted root")
        .hasSize(2);
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.EXISTING)
        .extracting(TrackedFile::location)
        .containsExactly(FILE_B.location());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.DELETED)
        .extracting(TrackedFile::location)
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
            .withMetrics(metrics(3, "c"))
            .build();

    // Snap 1: FILE_A and FILE_B land as direct rows.
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // Snap 2: overwrite drops FILE_A and adds FILE_C. FILE_B remains.
    table.newOverwrite().deleteFile(FILE_A).addFile(fileC).commit();

    Snapshot child = table.currentSnapshot();
    List<TrackedFile> rootRows = readRootManifestRows(child.snapshotFileLocation());
    List<TrackedFile> dataRows =
        rootRows.stream()
            .filter(r -> r.contentType() == FileContent.DATA)
            .collect(Collectors.toList());

    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.ADDED)
        .extracting(TrackedFile::location)
        .containsExactly(fileC.location());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.EXISTING)
        .extracting(TrackedFile::location)
        .containsExactly(FILE_B.location());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.DELETED)
        .extracting(TrackedFile::location)
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
    List<TrackedFile> rootRows = readRootManifestRows(child.snapshotFileLocation());
    List<TrackedFile> dataRows =
        rootRows.stream()
            .filter(r -> r.contentType() == FileContent.DATA)
            .collect(Collectors.toList());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.DELETED)
        .extracting(TrackedFile::location)
        .containsExactly(FILE_A.location());
    assertThat(dataRows)
        .filteredOn(r -> r.tracking().status() == EntryStatus.EXISTING)
        .extracting(TrackedFile::location)
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
            table.io().newInputFile(snap.snapshotFileLocation()),
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
    assertThat(snap.snapshotFileLocation()).isNotNull().endsWith(".parquet");
    // Phase 4d: the virtual manifest over the root's direct rows is the only data manifest.
    assertThat(snap.dataManifests(table.io()))
        .as("below-target buffer stays inline; only the virtual root-direct-rows manifest surfaces")
        .hasSize(1)
        .allSatisfy(m -> assertThat(m.path()).isEqualTo(snap.snapshotFileLocation()));

    List<TrackedFile> rootRows = readRootManifestRows(snap.snapshotFileLocation());
    assertThat(rootRows).as("both injected rows must land as root direct rows").hasSize(2);
    assertThat(rootRows)
        .allSatisfy(
            row -> {
              assertThat(row.contentType()).isEqualTo(FileContent.DATA);
              assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED);
            });
    assertThat(rootRows)
        .extracting(TrackedFile::location)
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
  }

  /**
   * With {@code commit.manifest.target-size-bytes=1}, every injected row projects over the target
   * and the accumulator spills; the resulting leaf-manifest-entries land in the root with status
   * ADDED. Verifies the spill branch of {@link SnapshotProducer#drainV4AdaptiveInputs}.
   */
  @Test
  public void testAdaptiveTreeDrainInjectedRowsSpillToLeaf() throws IOException {
    table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1").commit();

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

    List<TrackedFile> rootRows = readRootManifestRows(snap.snapshotFileLocation());
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
    table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1000").commit();

    List<DataFile> files = Lists.newArrayList();
    for (int i = 0; i < 7; i++) {
      files.add(
          DataFiles.builder(SPEC)
              .withPath("/path/to/data-mix-" + i + ".parquet")
              .withFileSizeInBytes(10)
              .withPartitionPath("data_bucket=" + i)
              .withMetrics(metrics(200 + i, "mix-" + i))
              .build());
    }
    AppendFiles append = table.newAppend();
    for (DataFile f : files) {
      append.appendFile(f);
    }
    append.commit();

    Snapshot snap = table.currentSnapshot();
    assertThat(snap.snapshotFileLocation()).isNotNull().endsWith(".parquet");

    // Root manifest carries 1 DATA_MANIFEST reference (the spilled leaf) + 2 DATA direct rows.
    List<TrackedFile> rootRows = readRootManifestRows(snap.snapshotFileLocation());
    List<TrackedFile> dataManifestRefs =
        rootRows.stream()
            .filter(r -> r.contentType() == FileContent.DATA_MANIFEST)
            .collect(Collectors.toList());
    List<TrackedFile> directDataRows =
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
            .filter(m -> m.path().equals(snap.snapshotFileLocation()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("virtual manifest missing"));
    ManifestFile realLeaf =
        dataManifests.stream()
            .filter(m -> !m.path().equals(snap.snapshotFileLocation()))
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
    table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1000").commit();

    // Snap 1: 7 files → one leaf of 5 (files 0..4) + two direct rows (files 5, 6).
    List<DataFile> parentFiles = Lists.newArrayList();
    for (int i = 0; i < 7; i++) {
      parentFiles.add(
          DataFiles.builder(SPEC)
              .withPath("/path/to/data-p-" + i + ".parquet")
              .withFileSizeInBytes(10)
              .withPartitionPath("data_bucket=" + i)
              .withMetrics(metrics(300 + i, "p-" + i))
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
            .withMetrics(metrics(400, "c-new"))
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
    List<TrackedFile> childRootRows = readRootManifestRows(child.snapshotFileLocation());
    assertThat(childRootRows)
        .filteredOn(
            r ->
                r.contentType() == FileContent.DATA && r.tracking().status() == EntryStatus.DELETED)
        .extracting(TrackedFile::location)
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
    assertThat(virtual.path()).isEqualTo(snap.snapshotFileLocation());
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
    // Strip DataFile-provided stats: the raw-inject helper path doesn't tolerate them (a separate
    // concern from real appends). The injected tests here don't assert on stats anyway.
    return TrackedFileAdapters.forDataFile(
            TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
            SCHEMA,
            metricsConfig,
            unionPartitionType)
        .wrap(file.copyWithoutStats(), tracking);
  }

  /**
   * A {@code CommitFailedException} retry re-invokes {@code MergingSnapshotProducer.apply}, which
   * re-runs {@link MergingSnapshotProducer#filterV4AdaptiveParentDirectRows} against the parent's
   * promoted-root direct rows. Without the per-apply reset introduced alongside this test, each
   * retry would re-append the retirement row to the accumulator's DELETED buffer, so a delete after
   * three commit failures would emit four DELETED direct rows for the same file — corrupting the
   * child snapshot's row set.
   */
  @Test
  public void testDirectRowDeleteIsIdempotentOnCommitRetry() throws IOException {
    // Snap 1: FILE_A and FILE_B land inline as direct rows on the promoted root.
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // Snap 2: retire FILE_A after three simulated commit failures.
    table.ops().failCommits(3);
    table.newDelete().deleteFile(FILE_A).commit();

    Snapshot child = table.currentSnapshot();
    List<TrackedFile> childRootRows = readRootManifestRows(child.snapshotFileLocation());
    assertThat(childRootRows)
        .filteredOn(
            r ->
                r.contentType() == FileContent.DATA && r.tracking().status() == EntryStatus.DELETED)
        .as("Retirement must be idempotent across commit retries")
        .extracting(TrackedFile::location)
        .containsExactly(FILE_A.location());
  }

  /**
   * The adaptive tree grows from one level to two across commits. A first small write keeps every
   * file inline as a root direct row (1-level: no on-disk leaf). A later commit whose carried
   * survivors plus newly-appended files cross {@code commit.manifest.target-size-bytes} pushes the
   * combined live pool over the spill threshold, so it rolls an on-disk leaf and the child root
   * references it (2-level). Exercises the carry-then-spill branch documented in {@link
   * SnapshotProducer#runAdaptiveDrainAndPromote} ("roll into leaves uniformly when the combined
   * live set is large").
   */
  @Test
  public void testAdaptiveTreeGrowsFromOneLevelToTwoLevelsAcrossCommits() throws IOException {
    // 200-byte seed against a 1000-byte target → the live pool rolls a leaf every 5 rows.
    table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1000").commit();

    // Snap 1: two files stay inline as root direct rows — a 1-level tree, no on-disk leaf.
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot parent = table.currentSnapshot();
    assertThat(realLeaves(parent))
        .as("first small write is a 1-level tree: root direct rows only, no on-disk leaf")
        .isEmpty();
    assertThat(parent.dataManifests(table.io()))
        .hasSize(1)
        .allSatisfy(m -> assertThat(m.path()).isEqualTo(parent.snapshotFileLocation()));

    // Snap 2: four more files. Carried survivors (2 EXISTING) + new (4 ADDED) = 6 live rows cross
    // the 5-row target, so the combined live pool spills into an on-disk leaf: the tree grows to
    // 2 levels.
    List<DataFile> more = newDataFiles("grow", 2, 4);
    AppendFiles append = table.newAppend();
    more.forEach(append::appendFile);
    append.commit();
    Snapshot child = table.currentSnapshot();

    assertThat(realLeaves(child))
        .as("combined live set crosses the target, so the child is a 2-level tree")
        .isNotEmpty();
    assertThat(readRootManifestRows(child.snapshotFileLocation()))
        .filteredOn(r -> r.contentType() == FileContent.DATA_MANIFEST)
        .as("2-level tree: the root carries at least one leaf reference")
        .isNotEmpty();

    // Scan planning surfaces every live file across both surfaces (root direct rows + leaf).
    List<String> expected = Lists.newArrayList(FILE_A.location(), FILE_B.location());
    more.forEach(f -> expected.add(f.location()));
    assertThat(planFileLocations()).containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * A first commit large enough to fully spill produces a 2-level tree whose root holds only leaf
   * references and no direct rows. Ten files against a five-row leaf target roll exactly two full
   * leaves with no sub-target remainder, so the promoted root carries zero DATA direct rows — the
   * pure-leaf counterpart to {@link #testAdaptiveTreeMixedSpillAndDirectRows}, which always leaves
   * a direct-row tail.
   */
  @Test
  public void testAdaptiveTreeFirstCommitFullySpillsWithNoDirectRows() throws IOException {
    table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1000").commit();

    List<DataFile> files = newDataFiles("pureleaf", 0, 10);
    AppendFiles append = table.newAppend();
    files.forEach(append::appendFile);
    append.commit();
    Snapshot snap = table.currentSnapshot();

    List<TrackedFile> rootRows = readRootManifestRows(snap.snapshotFileLocation());
    assertThat(rootRows)
        .filteredOn(r -> r.contentType() == FileContent.DATA)
        .as("an even multiple of the leaf target leaves no direct-row remainder")
        .isEmpty();
    assertThat(rootRows)
        .filteredOn(r -> r.contentType() == FileContent.DATA_MANIFEST)
        .as("first commit spills entirely into two on-disk leaves")
        .hasSize(2)
        .allSatisfy(r -> assertThat(r.tracking().status()).isEqualTo(EntryStatus.ADDED));

    // No virtual root-direct-rows manifest is synthesized — every data manifest is a real leaf.
    assertThat(snap.dataManifests(table.io()))
        .hasSize(2)
        .allSatisfy(m -> assertThat(m.path()).isNotEqualTo(snap.snapshotFileLocation()));

    assertThat(planFileLocations())
        .containsExactlyInAnyOrderElementsOf(
            files.stream().map(DataFile::location).collect(Collectors.toList()));
  }

  /**
   * Copy-on-write retirement of a data file that lives in an on-disk leaf rather than a root direct
   * row. With a small target the parent's first commit spills five files into a leaf plus a
   * direct-row tail; a child {@code newDelete} then retires one leaf-resident file. The leaf is
   * unpacked and re-drained through the accumulator ({@link
   * ManifestFilterManager#isV4AdaptiveMode}): the retired file becomes a DELETED entry, the
   * survivors carry forward, and no delete manifest is written. Complements the direct-row
   * retirements in {@link #testOverwriteWithMultipleParentDirectRowRetirements}, which only
   * exercise the direct-row filter path.
   */
  @Test
  public void testCopyOnWriteRetiresLeafResidentDataFile() throws IOException {
    table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1000").commit();

    // Snap 1: six files → one on-disk leaf of five (buckets 0..4) + one direct-row tail (bucket 5).
    List<DataFile> files = newDataFiles("cow", 0, 6);
    AppendFiles append = table.newAppend();
    files.forEach(append::appendFile);
    append.commit();
    Snapshot parent = table.currentSnapshot();

    DataFile leafResident = files.get(2);
    List<ManifestFile> parentLeaves = realLeaves(parent);
    assertThat(parentLeaves).as("parent must have exactly one real on-disk leaf").hasSize(1);
    assertThat(readLeafManifestEntries(parentLeaves.get(0)))
        .as("the file to retire must be leaf-resident, not a root direct row")
        .extracting(e -> e.file().location())
        .contains(leafResident.location());

    // Snap 2: copy-on-write delete of the leaf-resident file — no delete files involved.
    table.newDelete().deleteFile(leafResident).commit();
    Snapshot child = table.currentSnapshot();

    assertThat(child.deleteManifests(table.io()))
        .as("copy-on-write must not introduce delete manifests")
        .isEmpty();

    // The retired leaf-resident file surfaces as a DELETED direct row on the child root, proving
    // the
    // leaf was unpacked and routed through the accumulator (not the direct-row filter path).
    assertThat(readRootManifestRows(child.snapshotFileLocation()))
        .filteredOn(
            r ->
                r.contentType() == FileContent.DATA && r.tracking().status() == EntryStatus.DELETED)
        .extracting(TrackedFile::location)
        .containsExactly(leafResident.location());

    // Scan planning surfaces exactly the survivors.
    List<String> survivors =
        files.stream()
            .map(DataFile::location)
            .filter(loc -> !loc.equals(leafResident.location()))
            .collect(Collectors.toList());
    assertThat(planFileLocations()).containsExactlyInAnyOrderElementsOf(survivors);
  }

  /**
   * Unpartitioned v4 tables: the empty partition type maps to {@code UnknownType}, so no partition
   * column (and no {@code _unpartitioned} placeholder) is written — the value is null on read.
   * Component-level null-partition round-trips are covered by {@code TestV4ManifestReader}; this
   * exercises the full commit path for both surfaces (a small write that stays a root direct row,
   * and a spilled on-disk leaf) and confirms scan planning round-trips.
   */
  @Test
  public void testUnpartitionedV4RoundTrips() throws IOException {
    Schema schema =
        new Schema(
            required(3, "id", Types.IntegerType.get()),
            required(4, "data", Types.StringType.get()));
    TestTables.TestTable unpart =
        TestTables.create(
            new File(tableDir, "unpart"),
            "unpart",
            schema,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            4);

    DataFile file1 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/u-1.parquet")
            .withFileSizeInBytes(10)
            .withMetrics(metrics(500, "u-1"))
            .build();

    // Small write → root direct row. The unpartitioned root manifest is readable with an
    // empty-partition (UnknownType) schema, and the row decodes.
    unpart.newAppend().appendFile(file1).commit();
    Snapshot direct = unpart.currentSnapshot();

    List<TrackedFile> rootRows = readRootManifestRows(direct.snapshotFileLocation());
    assertThat(rootRows).hasSize(1);
    assertThat(rootRows.get(0).contentType()).isEqualTo(FileContent.DATA);
    assertThat(rootRows.get(0).location()).isEqualTo(file1.location());

    try (CloseableIterable<FileScanTask> tasks = unpart.newScan().planFiles()) {
      assertThat(Lists.newArrayList(tasks))
          .extracting(t -> t.file().location())
          .containsExactly(file1.location());
    }

    // Spill to an on-disk leaf (target=1) and confirm the unpartitioned leaf round-trips too.
    unpart.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1").commit();
    DataFile file2 =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/path/to/u-2.parquet")
            .withFileSizeInBytes(10)
            .withMetrics(metrics(501, "u-2"))
            .build();
    unpart.newAppend().appendFile(file2).commit();
    Snapshot spilled = unpart.currentSnapshot();

    assertThat(spilled.dataManifests(unpart.io()))
        .filteredOn(m -> !m.path().equals(spilled.snapshotFileLocation()))
        .as("target=1 spills unpartitioned rows into on-disk leaves")
        .isNotEmpty();

    try (CloseableIterable<FileScanTask> tasks = unpart.newScan().planFiles()) {
      assertThat(Lists.newArrayList(tasks))
          .extracting(t -> t.file().location())
          .containsExactlyInAnyOrder(file1.location(), file2.location());
    }
  }
}
