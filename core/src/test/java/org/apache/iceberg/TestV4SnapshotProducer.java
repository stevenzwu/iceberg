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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
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
   * First append to a v4 table:
   *
   * <ul>
   *   <li>Snapshot has rootManifestLocation set (.parquet), manifestListLocation null.
   *   <li>Root manifest has one DATA_MANIFEST entry with format_version=4, status=ADDED.
   *   <li>The leaf manifest path in the root entry matches the leaf written by the snapshot.
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

    // root manifest must carry exactly one DATA_MANIFEST entry
    List<TrackedFileStruct> rootRows = readRootManifestRows(snap.rootManifestLocation());
    assertThat(rootRows).hasSize(1);

    TrackedFileStruct rootEntry = rootRows.get(0);
    assertThat(rootEntry.contentType())
        .as("root entry must be DATA_MANIFEST")
        .isEqualTo(FileContent.DATA_MANIFEST);
    assertThat(rootEntry.formatVersion())
        .as("format_version must be 4 for v4 leaf")
        .isEqualTo(TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE);

    // tracking status: newly written by this snapshot => ADDED
    Tracking tracking = rootEntry.tracking();
    assertThat(tracking).isNotNull();
    assertThat(tracking.status())
        .as("root entry for new manifest must be ADDED")
        .isEqualTo(EntryStatus.ADDED);

    // the leaf referred to by the root entry is the only data manifest in the snapshot
    List<ManifestFile> dataManifests = snap.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    assertThat(rootEntry.location())
        .as("root entry location must match the leaf manifest path")
        .isEqualTo(dataManifests.get(0).path());
  }

  /**
   * With {@code commit.manifest.adaptive-tree.enabled=true}, FastAppend routes new DataFiles into
   * the accumulator input channel. Two entries against the default 8 MB target project well
   * below the spill threshold, so both stay inline as root direct rows and no leaf manifest is
   * written.
   */
  @Test
  public void testAppendV4AdaptiveTreeSmallWriteStaysInline() throws IOException {
    table.updateProperties().set(TableProperties.MANIFEST_ADAPTIVE_TREE_ENABLED, "true").commit();

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

    assertThat(snap.dataManifests(table.io()))
        .as("below-target buffer stays inline: no leaf manifest is written")
        .isEmpty();

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
   * Exercises the v4 adaptive-tree drain path by injecting synthetic {@link TrackedFile} rows into
   * {@link SnapshotProducer#addV4AdaptiveNewLiveDataRow} before commit. The 8 MB default target is
   * far above two 200-byte projected rows, so both stay as root direct rows and no leaf manifest
   * is written.
   */
  @Test
  public void testAdaptiveTreeDrainInjectedRowsAsDirectRows() throws IOException {
    table.updateProperties().set(TableProperties.MANIFEST_ADAPTIVE_TREE_ENABLED, "true").commit();

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
    assertThat(snap.dataManifests(table.io()))
        .as("below-target buffer stays inline: no leaf manifests written")
        .isEmpty();

    List<TrackedFileStruct> rootRows = readRootManifestRows(snap.rootManifestLocation());
    assertThat(rootRows)
        .as("both injected rows must land as root direct rows")
        .hasSize(2);
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
        .set(TableProperties.MANIFEST_ADAPTIVE_TREE_ENABLED, "true")
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
    assertThat(leaves)
        .as("with target-size=1 every row exceeds the spill threshold")
        .isNotEmpty();

    List<TrackedFileStruct> rootRows = readRootManifestRows(snap.rootManifestLocation());
    assertThat(rootRows)
        .filteredOn(row -> row.contentType() == FileContent.DATA_MANIFEST)
        .as("spilled leaves surface as DATA_MANIFEST references in the root")
        .isNotEmpty()
        .allSatisfy(
            row -> assertThat(row.tracking().status()).isEqualTo(EntryStatus.ADDED));
  }

  private TrackedFile buildInjectedDataRow(DataFile file, long snapId) {
    Types.StructType unionPartitionType =
        Partitioning.unionPartitionTypes(table.specs().values());
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
