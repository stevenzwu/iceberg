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

import static org.apache.iceberg.V4TestHelpers.ADDED_TRACKING;
import static org.apache.iceberg.V4TestHelpers.SNAPSHOT_ID;
import static org.apache.iceberg.V4TestHelpers.dataFile;
import static org.apache.iceberg.V4TestHelpers.dataFileWithStats;
import static org.apache.iceberg.V4TestHelpers.deletionVector;
import static org.apache.iceberg.V4TestHelpers.manifestRef;
import static org.apache.iceberg.V4TestHelpers.manifestRefWithStats;
import static org.apache.iceberg.V4TestHelpers.partition;
import static org.apache.iceberg.V4TestHelpers.readTrackedFiles;
import static org.apache.iceberg.V4TestHelpers.writeTrackedFiles;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestV4TrackedFileWrappers {
  private static final long COMMIT_SNAPSHOT_ID = SNAPSHOT_ID;
  private static final long COMMIT_SEQUENCE_NUMBER = 34L;
  private static final long PREVIOUS_SNAPSHOT_ID = 7L;
  private static final long PREVIOUS_SEQUENCE_NUMBER = 5L;
  private static final long FIRST_ROW_ID = 1_000L;

  private static final Schema TABLE_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec ID_SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA).withSpecId(1).identity("id").build();
  private static final PartitionSpec DATA_SPEC =
      PartitionSpec.builderFor(TABLE_SCHEMA)
          .withSpecId(2)
          .add(2 /* sourceId */, 1001 /* fieldId */, "data", Transforms.identity())
          .build();
  private static final Map<Integer, PartitionSpec> ID_SPECS =
      ImmutableMap.of(ID_SPEC.specId(), ID_SPEC);
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());

  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(
          TABLE_SCHEMA, MetricsTestUtil.from(ImmutableMap.of(), TABLE_SCHEMA));
  private static final FieldStats<Integer> ID_STATS =
      new FieldStatsStruct<>(
          STATS_TYPE.fieldType("id").asStructType(),
          1 /* lowerBound */,
          99 /* upperBound */,
          true /* tightBounds */,
          100L /* valueCount */,
          0L /* nullValueCount */,
          0L /* nanValueCount */,
          0L /* totalBytes */);
  private static final FieldStats<String> DATA_STATS =
      new FieldStatsStruct<>(
          STATS_TYPE.fieldType("data").asStructType(),
          "a",
          "z",
          false /* tightBounds */,
          100L /* valueCount */,
          10L /* nullValueCount */,
          0L /* nanValueCount */,
          500L /* totalBytes */);

  private static final Tracking EXISTING_TRACKING =
      new TrackingStruct(
          EntryStatus.EXISTING,
          PREVIOUS_SNAPSHOT_ID,
          PREVIOUS_SEQUENCE_NUMBER,
          PREVIOUS_SEQUENCE_NUMBER,
          null /* modifiedSnapshotId */,
          FIRST_ROW_ID,
          null /* deletedPositions */,
          null /* replacedPositions */);
  private static final ManifestInfo MANIFEST_INFO =
      new ManifestInfoStruct(
          49 /* addedFilesCount */,
          51 /* existingFilesCount */,
          0 /* deletedFilesCount */,
          0 /* replacedFilesCount */,
          0 /* modifiedFilesCount */,
          4_900L /* addedRowsCount */,
          5_100L /* existingRowsCount */,
          0L /* deletedRowsCount */,
          0L /* replacedRowsCount */,
          0L /* modifiedRowsCount */,
          PREVIOUS_SEQUENCE_NUMBER,
          null /* dv */,
          4 /* formatVersion */);

  private final InMemoryFileIO io = new InMemoryFileIO();

  @Test
  void leafWritesDataFile() throws IOException {
    TrackedFile expected =
        dataFile(
            EXISTING_TRACKING,
            "s3://bucket/table/id=1/file.parquet",
            ID_SPEC.specId(),
            partition(ID_SPEC, 1),
            null /* stats */,
            deletionVector("s3://bucket/table/dv.puffin"));

    TrackedFile actual =
        writeAndRead(ID_SPEC.partitionType(), leaf(ID_SPECS).wrap(expected, expected.tracking()));

    assertThat(actual)
        .usingComparator(V4TestComparators.trackedFile(ID_SPEC.partitionType()))
        .isEqualTo(expected);
  }

  @Test
  void leafWritesNullSequenceNumbersForAddedFile() throws IOException {
    TrackedFile file = dataFile(ADDED_TRACKING, "s3://bucket/table/file.parquet");

    Tracking actual =
        writeAndRead(UNPARTITIONED_SPECS, leaf(UNPARTITIONED_SPECS).wrap(file, ADDED_TRACKING));

    assertThat(actual.snapshotId()).isEqualTo(COMMIT_SNAPSHOT_ID);
    assertThat(actual.dataSequenceNumber()).isNull();
    assertThat(actual.fileSequenceNumber()).isNull();
    assertThat(actual.firstRowId()).isNull();
  }

  @Test
  void leafWritesAdaptedManifestEntry() throws IOException {
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("s3://bucket/table/file.parquet")
            .withFileSizeInBytes(1024L)
            .withRecordCount(100L)
            .withFirstRowId(FIRST_ROW_ID)
            .build();
    ManifestEntry<DataFile> entry =
        new GenericManifestEntry<DataFile>(
                ManifestEntry.getSchema(Types.StructType.of()).asStruct())
            .wrapExisting(
                PREVIOUS_SNAPSHOT_ID, PREVIOUS_SEQUENCE_NUMBER, PREVIOUS_SEQUENCE_NUMBER, dataFile);
    TrackedFile file = TrackedFileAdapters.forDataFile(TABLE_SCHEMA).wrap(entry);

    Tracking actual =
        writeAndRead(UNPARTITIONED_SPECS, leaf(UNPARTITIONED_SPECS).wrap(file, file.tracking()));

    assertThat(actual).usingComparator(V4TestComparators.TRACKING).isEqualTo(EXISTING_TRACKING);
  }

  @Test
  void leafWritesStatsFromAdaptedDataFile() throws IOException {
    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("s3://bucket/table/file.parquet")
            .withFileSizeInBytes(1024L)
            .withMetrics(
                new Metrics(
                    100L /* rowCount */,
                    null /* columnSizes */,
                    ImmutableMap.of(1, 100L, 2, 100L),
                    ImmutableMap.of(1, 0L, 2, 10L),
                    null /* nanValueCounts */,
                    ImmutableMap.of(
                        1, Conversions.toByteBuffer(Types.IntegerType.get(), 1),
                        2, Conversions.toByteBuffer(Types.StringType.get(), "a")),
                    ImmutableMap.of(
                        1, Conversions.toByteBuffer(Types.IntegerType.get(), 99),
                        2, Conversions.toByteBuffer(Types.StringType.get(), "z"))))
            .build();
    TrackedFile file = TrackedFileAdapters.forDataFile(TABLE_SCHEMA).wrap(dataFile);

    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(),
            STATS_TYPE,
            leaf(UNPARTITIONED_SPECS, STATS_TYPE).wrap(file, ADDED_TRACKING));

    assertThat(actual.contentStats())
        .usingComparator(V4TestComparators.CONTENT_STATS)
        .isEqualTo(file.contentStats());
  }

  @Test
  void leafWritesStatsUsingManifestStatsType() throws IOException {
    Types.StructType reorderedType =
        Types.StructType.of(STATS_TYPE.field("data"), STATS_TYPE.field("id"));
    ContentStats stats = StatsTestUtil.contentStats(reorderedType, ID_STATS, DATA_STATS);
    TrackedFile file = dataFileWithStats("s3://bucket/table/file.parquet", stats);

    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(),
            STATS_TYPE,
            leaf(UNPARTITIONED_SPECS, STATS_TYPE).wrap(file, ADDED_TRACKING));

    assertThat(actual.contentStats())
        .usingComparator(V4TestComparators.CONTENT_STATS)
        .isEqualTo(stats);
  }

  @Test
  void leafWritesNullStatsForColumnWithoutStats() throws IOException {
    ContentStats stats = StatsTestUtil.contentStats(STATS_TYPE, ID_STATS);
    TrackedFile file = dataFileWithStats("s3://bucket/table/file.parquet", stats);

    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(),
            STATS_TYPE,
            leaf(UNPARTITIONED_SPECS, STATS_TYPE).wrap(file, ADDED_TRACKING));

    assertThat(actual.contentStats().statsFor(DATA_STATS.fieldId())).isNull();
  }

  @Test
  void leafWritesGeospatialBounds() throws IOException {
    Schema geoSchema = new Schema(optional(1, "geom", Types.GeometryType.crs84()));
    Types.StructType geoStatsType =
        StatsUtil.statsWriteSchema(geoSchema, MetricsTestUtil.from(ImmutableMap.of(), geoSchema));
    GeospatialBound lower = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound upper = GeospatialBound.createXYZ(3.0, 4.0, 5.0);
    FieldStats<GeospatialBound> geoStats =
        new FieldStatsStruct<>(
            geoStatsType.fieldType("geom").asStructType(),
            lower,
            upper,
            false /* tightBounds */,
            10L /* valueCount */,
            0L /* nullValueCount */,
            0L /* nanValueCount */,
            100L /* totalBytes */);
    TrackedFile file =
        dataFileWithStats(
            "s3://bucket/table/file.parquet", StatsTestUtil.contentStats(geoStatsType, geoStats));

    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(),
            geoStatsType,
            leaf(UNPARTITIONED_SPECS, geoStatsType).wrap(file, ADDED_TRACKING));

    FieldStats<GeospatialBound> actualStats = actual.contentStats().statsFor(geoStats.fieldId());
    assertThat(actualStats.lowerBound()).isEqualTo(lower);
    assertThat(actualStats.upperBound()).isEqualTo(upper);
  }

  @Test
  void leafProjectsPartitionToUnionType() throws IOException {
    Map<Integer, PartitionSpec> specs =
        ImmutableMap.of(ID_SPEC.specId(), ID_SPEC, DATA_SPEC.specId(), DATA_SPEC);
    Types.StructType unionType = Partitioning.unionPartitionTypes(specs.values());
    TrackedFile file =
        dataFile(
            "s3://bucket/table/data=a/file.parquet", DATA_SPEC.specId(), partition(DATA_SPEC, "a"));

    TrackedFile actual = writeAndRead(unionType, leaf(specs).wrap(file, ADDED_TRACKING));

    PartitionData expected = new PartitionData(unionType);
    expected.set(unionType.fields().indexOf(unionType.field("data")), "a");
    assertThat(actual.partition())
        .usingComparator(Comparators.forType(unionType))
        .isEqualTo(expected);
  }

  @Test
  void leafReturnsPartitionWithSpecType() {
    Map<Integer, PartitionSpec> specs =
        ImmutableMap.of(ID_SPEC.specId(), ID_SPEC, DATA_SPEC.specId(), DATA_SPEC);
    PartitionData partition = partition(DATA_SPEC, "a");
    TrackedFile file =
        dataFile("s3://bucket/table/data=a/file.parquet", DATA_SPEC.specId(), partition);

    TrackedFile wrapped = leaf(specs).wrap(file, ADDED_TRACKING);

    assertThat(wrapped.partition())
        .usingComparator(Comparators.forType(DATA_SPEC.partitionType()))
        .isEqualTo(partition);
  }

  @Test
  void leafRejectsUnassignedSequenceNumberForExistingFile() {
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.EXISTING,
            COMMIT_SNAPSHOT_ID,
            null /* dataSequenceNumber */,
            null /* fileSequenceNumber */,
            null /* modifiedSnapshotId */,
            null /* firstRowId */,
            null /* deletedPositions */,
            null /* replacedPositions */);
    TrackedFile file = dataFile(tracking, "s3://bucket/table/file.parquet");

    assertThatThrownBy(() -> leaf(UNPARTITIONED_SPECS).wrap(file, tracking))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot write file with null sequence number and status %s (must be ADDED): %s",
            EntryStatus.EXISTING, file.location());
  }

  @Test
  void leafRejectsManifest() {
    TrackedFile manifest =
        manifestRef(FileContent.DATA_MANIFEST, "s3://bucket/table/leaf.parquet", MANIFEST_INFO);

    assertThatThrownBy(() -> leaf(UNPARTITIONED_SPECS).wrap(manifest, ADDED_TRACKING))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid content type for leaf manifest: DATA_MANIFEST");
  }

  @Test
  void rootPreservesAssignedValues() throws IOException {
    TrackedFile manifest =
        manifestRef(
            EXISTING_TRACKING,
            FileContent.DATA_MANIFEST,
            "s3://bucket/table/leaf.parquet",
            MANIFEST_INFO);

    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(),
            root(UNPARTITIONED_SPECS)
                .wrap(manifest, manifest.tracking(), null /* assignedFirstRowId */));

    assertThat(actual)
        .usingComparator(V4TestComparators.trackedFile(Types.StructType.of()))
        .isEqualTo(manifest);
  }

  @Test
  void rootWritesManifestStats() throws IOException {
    ContentStats stats = StatsTestUtil.contentStats(STATS_TYPE, ID_STATS, DATA_STATS);
    TrackedFile manifest =
        manifestRefWithStats(
            FileContent.DATA_MANIFEST, "s3://bucket/table/leaf.parquet", stats, MANIFEST_INFO);

    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(),
            STATS_TYPE,
            root(UNPARTITIONED_SPECS, STATS_TYPE)
                .wrap(manifest, EXISTING_TRACKING, null /* assignedFirstRowId */));

    assertThat(actual.contentStats())
        .usingComparator(V4TestComparators.CONTENT_STATS)
        .isEqualTo(stats);
  }

  @Test
  void rootAssignsCommitSequenceNumberToNewManifest() throws IOException {
    ManifestFile manifest = newManifest(ManifestContent.DATA, COMMIT_SNAPSHOT_ID);
    TrackedFile file = TrackedFileAdapters.forManifestFile().wrap(manifest);

    TrackedFile actual =
        writeAndRead(
            Types.StructType.of(),
            root(UNPARTITIONED_SPECS).wrap(file, file.tracking(), FIRST_ROW_ID));

    assertThat(actual.tracking().dataSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
    assertThat(actual.tracking().fileSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
    assertThat(actual.manifestInfo().minSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
  }

  @Test
  void rootReturnsAssignedValues() {
    ManifestFile manifest = newManifest(ManifestContent.DATA, COMMIT_SNAPSHOT_ID);
    TrackedFile file = TrackedFileAdapters.forManifestFile().wrap(manifest);

    TrackedFile wrapped = root(UNPARTITIONED_SPECS).wrap(file, file.tracking(), FIRST_ROW_ID);

    assertThat(wrapped.tracking().dataSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
    assertThat(wrapped.tracking().fileSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
    assertThat(wrapped.tracking().firstRowId()).isEqualTo(FIRST_ROW_ID);
    assertThat(wrapped.manifestInfo().minSequenceNumber()).isEqualTo(COMMIT_SEQUENCE_NUMBER);
  }

  @Test
  void rootCopyIsIndependentOfWrapperReuse() throws IOException {
    TrackedFile file = dataFile(ADDED_TRACKING, "s3://bucket/table/file.parquet");
    V4Metadata.RootTrackedFileWrapper wrapper = root(UNPARTITIONED_SPECS);
    TrackedFile expected =
        writeAndRead(Types.StructType.of(), wrapper.wrap(file, ADDED_TRACKING, FIRST_ROW_ID));

    TrackedFile copy = wrapper.wrap(file, ADDED_TRACKING, FIRST_ROW_ID).copy();
    wrapper.wrap(
        manifestRef(FileContent.DELETE_MANIFEST, "s3://bucket/table/deletes.avro", MANIFEST_INFO),
        EXISTING_TRACKING,
        null /* assignedFirstRowId */);

    assertThat(copy)
        .usingComparator(V4TestComparators.trackedFile(Types.StructType.of()))
        .isEqualTo(expected);
  }

  @Test
  void rootAssignsCommitSnapshotIdWhenUnset() throws IOException {
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            null /* snapshotId */,
            null /* dataSequenceNumber */,
            null /* fileSequenceNumber */,
            null /* modifiedSnapshotId */,
            null /* firstRowId */,
            null /* deletedPositions */,
            null /* replacedPositions */);
    TrackedFile file = dataFile(tracking, "s3://bucket/table/file.parquet");

    Tracking actual =
        writeAndRead(
            UNPARTITIONED_SPECS,
            root(UNPARTITIONED_SPECS).wrap(file, file.tracking(), FIRST_ROW_ID));

    assertThat(actual.snapshotId()).isEqualTo(COMMIT_SNAPSHOT_ID);
  }

  @Test
  void rootAssignsFirstRowIdToAddedDataFile() throws IOException {
    TrackedFile file = dataFile(ADDED_TRACKING, "s3://bucket/table/file.parquet");

    Tracking actual =
        writeAndRead(
            UNPARTITIONED_SPECS,
            root(UNPARTITIONED_SPECS).wrap(file, file.tracking(), FIRST_ROW_ID));

    assertThat(actual.firstRowId()).isEqualTo(FIRST_ROW_ID);
  }

  @Test
  void rootWritesNullFirstRowIdForDeleteManifest() throws IOException {
    TrackedFile manifest =
        manifestRef(FileContent.DELETE_MANIFEST, "s3://bucket/table/deletes.avro", MANIFEST_INFO);

    Tracking actual =
        writeAndRead(
            UNPARTITIONED_SPECS,
            root(UNPARTITIONED_SPECS)
                .wrap(manifest, EXISTING_TRACKING, null /* assignedFirstRowId */));

    assertThat(actual.firstRowId()).isNull();
  }

  @Test
  void rootRejectsUnassignedSequenceNumberFromAnotherSnapshot() {
    ManifestFile manifest = newManifest(ManifestContent.DATA, PREVIOUS_SNAPSHOT_ID);
    TrackedFile file = TrackedFileAdapters.forManifestFile().wrap(manifest);

    assertThatThrownBy(() -> root(UNPARTITIONED_SPECS).wrap(file, file.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot assign sequence number to file from snapshot %s (committing snapshot %s): %s",
            PREVIOUS_SNAPSHOT_ID, COMMIT_SNAPSHOT_ID, manifest.path());
  }

  @Test
  void rootRejectsUnassignedFirstRowId() {
    TrackedFile file = dataFile(ADDED_TRACKING, "s3://bucket/table/file.parquet");

    assertThatThrownBy(
            () ->
                root(UNPARTITIONED_SPECS).wrap(file, ADDED_TRACKING, null /* assignedFirstRowId */))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Missing first-row-id for file: %s", file.location());
  }

  @Test
  void rootRejectsFirstRowIdAssignmentForDeletedFile() {
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.DELETED,
            COMMIT_SNAPSHOT_ID,
            PREVIOUS_SEQUENCE_NUMBER,
            PREVIOUS_SEQUENCE_NUMBER,
            null /* modifiedSnapshotId */,
            null /* firstRowId */,
            null /* deletedPositions */,
            null /* replacedPositions */);
    TrackedFile file = dataFile(tracking, "s3://bucket/table/file.parquet");

    assertThatThrownBy(() -> root(UNPARTITIONED_SPECS).wrap(file, tracking, FIRST_ROW_ID))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot assign first-row-id to file with status %s: %s",
            EntryStatus.DELETED, file.location());
  }

  @Test
  void rootRejectsFirstRowIdAssignmentForAssignedFile() {
    TrackedFile file = dataFile(EXISTING_TRACKING, "s3://bucket/table/file.parquet");

    assertThatThrownBy(() -> root(UNPARTITIONED_SPECS).wrap(file, file.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot assign first-row-id to file with existing first-row-id %s: %s",
            EXISTING_TRACKING.firstRowId(), file.location());
  }

  @Test
  void rootRejectsFirstRowIdAssignmentForDeleteManifest() {
    TrackedFile manifest =
        manifestRef(FileContent.DELETE_MANIFEST, "s3://bucket/table/deletes.avro", MANIFEST_INFO);

    assertThatThrownBy(
            () -> root(UNPARTITIONED_SPECS).wrap(manifest, manifest.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot assign first-row-id to delete manifest: %s", manifest.location());
  }

  @Test
  void rootRejectsManifestWithoutManifestInfo() {
    TrackedFile manifest =
        manifestRef(FileContent.DATA_MANIFEST, "s3://bucket/table/leaf.parquet", null);

    assertThatThrownBy(
            () -> root(UNPARTITIONED_SPECS).wrap(manifest, manifest.tracking(), FIRST_ROW_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing manifest info for manifest: %s", manifest.location());
  }

  private static V4Metadata.LeafTrackedFileWrapper leaf(Map<Integer, PartitionSpec> specsById) {
    return leaf(specsById, Types.StructType.of());
  }

  private static V4Metadata.LeafTrackedFileWrapper leaf(
      Map<Integer, PartitionSpec> specsById, Types.StructType statsType) {
    return new V4Metadata.LeafTrackedFileWrapper(specsById, statsType);
  }

  private static V4Metadata.RootTrackedFileWrapper root(Map<Integer, PartitionSpec> specsById) {
    return root(specsById, Types.StructType.of());
  }

  private static V4Metadata.RootTrackedFileWrapper root(
      Map<Integer, PartitionSpec> specsById, Types.StructType statsType) {
    return new V4Metadata.RootTrackedFileWrapper(
        COMMIT_SNAPSHOT_ID, COMMIT_SEQUENCE_NUMBER, specsById, statsType);
  }

  private static ManifestFile newManifest(ManifestContent content, long snapshotId) {
    return new GenericManifestFile(
        "s3://bucket/table/new-manifest.avro",
        1024L /* length */,
        PartitionSpec.unpartitioned().specId(),
        content,
        ManifestWriter.UNASSIGNED_SEQ /* sequenceNumber */,
        ManifestWriter.UNASSIGNED_SEQ /* minSequenceNumber */,
        snapshotId,
        ImmutableList.of(),
        null /* keyMetadata */,
        1 /* addedFilesCount */,
        100L /* addedRowsCount */,
        0 /* existingFilesCount */,
        0L /* existingRowsCount */,
        0 /* deletedFilesCount */,
        0L /* deletedRowsCount */,
        null /* firstRowId */);
  }

  private Tracking writeAndRead(Map<Integer, PartitionSpec> specsById, TrackedFile file)
      throws IOException {
    return writeAndRead(Partitioning.unionPartitionTypes(specsById.values()), file).tracking();
  }

  private TrackedFile writeAndRead(Types.StructType partitionType, TrackedFile file)
      throws IOException {
    return writeAndRead(partitionType, Types.StructType.of(), file);
  }

  private TrackedFile writeAndRead(
      Types.StructType partitionType, Types.StructType statsType, TrackedFile file)
      throws IOException {
    OutputFile out =
        writeTrackedFiles(io, FileFormat.PARQUET, partitionType, statsType, ImmutableList.of(file));
    return Iterables.getOnlyElement(
        readTrackedFiles(out.toInputFile(), FileFormat.PARQUET, partitionType, statsType));
  }
}
