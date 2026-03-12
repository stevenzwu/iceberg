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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRowLineageMetadata {
  @Parameters(name = "formatVersion = {0}")
  private static List<Integer> formatVersion() {
    return TestHelpers.V3_AND_ABOVE;
  }

  @Parameter private int formatVersion;

  private static final String TEST_LOCATION = "s3://bucket/test/location";

  private static final Schema TEST_SCHEMA =
      new Schema(
          7,
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  private TableMetadata baseMetadata() {
    return TableMetadata.buildFromEmpty(formatVersion)
        .addSchema(TEST_SCHEMA)
        .setLocation(TEST_LOCATION)
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .build();
  }

  @TempDir private File tableDir = null;

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void testSnapshotRowIDValidation() {
    Snapshot snapshot =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "ml.avro", null, null, null);
    assertThat(snapshot.firstRowId()).isNull();
    assertThat(snapshot.addedRows()).isNull();

    // added-rows will be set to null if first-row-id is null
    snapshot =
        new BaseSnapshot(0, 1, null, 0, DataOperations.APPEND, null, 1, "ml.avro", null, 10L, null);
    assertThat(snapshot.firstRowId()).isNull();
    assertThat(snapshot.addedRows()).isNull();

    // added-rows and first-row-id can be 0
    snapshot =
        new BaseSnapshot(0, 1, null, 0, DataOperations.APPEND, null, 1, "ml.avro", 0L, 0L, null);
    assertThat(snapshot.firstRowId()).isEqualTo(0);
    assertThat(snapshot.addedRows()).isEqualTo(0);

    assertThatThrownBy(
            () ->
                new BaseSnapshot(
                    0, 1, null, 0, DataOperations.APPEND, null, 1, "ml.avro", 10L, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid added-rows (required when first-row-id is set): null");

    assertThatThrownBy(
            () ->
                new BaseSnapshot(
                    0, 1, null, 0, DataOperations.APPEND, null, 1, "ml.avro", 0L, -1L, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid added-rows (cannot be negative): -1");

    assertThatThrownBy(
            () ->
                new BaseSnapshot(
                    0, 1, null, 0, DataOperations.APPEND, null, 1, "ml.avro", -1L, 1L, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid first-row-id (cannot be negative): -1");
  }

  @TestTemplate
  public void testSnapshotAddition() {
    long newRows = 30L;

    TableMetadata base = baseMetadata();

    Snapshot addRows =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.nextRowId(), newRows, null);

    TableMetadata firstAddition = TableMetadata.buildFrom(base).addSnapshot(addRows).build();

    assertThat(firstAddition.nextRowId()).isEqualTo(newRows);

    Snapshot addMoreRows =
        new BaseSnapshot(
            1,
            2,
            1L,
            0,
            DataOperations.APPEND,
            null,
            1,
            "foo",
            firstAddition.nextRowId(),
            newRows,
            null);

    TableMetadata secondAddition =
        TableMetadata.buildFrom(firstAddition).addSnapshot(addMoreRows).build();

    assertThat(secondAddition.nextRowId()).isEqualTo(newRows * 2);
  }

  @TestTemplate
  public void testInvalidSnapshotAddition() {
    Long newRows = 30L;

    TableMetadata base = baseMetadata();

    Snapshot invalidLastRow =
        new BaseSnapshot(0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", null, newRows, null);

    assertThatThrownBy(() -> TableMetadata.buildFrom(base).addSnapshot(invalidLastRow))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot add a snapshot: first-row-id is null");

    // add rows to check TableMetadata validation; Snapshot rejects negative next-row-id
    Snapshot addRows =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.nextRowId(), newRows, null);
    TableMetadata added = TableMetadata.buildFrom(base).addSnapshot(addRows).build();

    Snapshot invalidNewRows =
        new BaseSnapshot(
            1, 2, 1L, 0, DataOperations.APPEND, null, 1, "foo", added.nextRowId() - 1, 10L, null);

    assertThatThrownBy(() -> TableMetadata.buildFrom(added).addSnapshot(invalidNewRows))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "Cannot add a snapshot, first-row-id is behind table next-row-id: 29 < 30");
  }

  @TestTemplate
  public void testFastAppend() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    table.newFastAppend().appendFile(fileWithRows(30)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    table.newFastAppend().appendFile(fileWithRows(17)).appendFile(fileWithRows(11)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30 + 17 + 11);
  }

  @TestTemplate
  public void testAppend() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    table.newAppend().appendFile(fileWithRows(30)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    table.newAppend().appendFile(fileWithRows(17)).appendFile(fileWithRows(11)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30 + 17 + 11);
  }

  @TestTemplate
  public void testAppendBranch() {
    // Appends to a branch should still change last-row-id even if not on main, these changes
    // should also affect commits to main

    String branch = "some_branch";

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    // Write to Branch
    table.newAppend().appendFile(fileWithRows(30)).toBranch(branch).commit();

    assertThat(table.currentSnapshot()).isNull();
    assertThat(table.snapshot(branch).firstRowId()).isEqualTo(0L);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    // Write to Main
    table.newAppend().appendFile(fileWithRows(17)).appendFile(fileWithRows(11)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30 + 17 + 11);

    // Write again to branch
    table.newAppend().appendFile(fileWithRows(21)).toBranch(branch).commit();
    assertThat(table.snapshot(branch).firstRowId()).isEqualTo(30 + 17 + 11);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30 + 17 + 11 + 21);
  }

  @TestTemplate
  public void testDeletes() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    DataFile file = fileWithRows(30);

    table.newAppend().appendFile(file).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    table.newDelete().deleteFile(file).commit();

    // Deleting a file should create a new snapshot which should inherit last-row-id from the
    // previous metadata and not change last-row-id for this metadata.
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.currentSnapshot().addedRows()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);
  }

  @TestTemplate
  public void testPositionDeletes() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    DataFile file = fileWithRows(30);

    table.newAppend().appendFile(file).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    // v3 only allows Puffin-based DVs for position deletes
    DeleteFile deletes =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofPositionDeletes()
            .withFormat(FileFormat.PUFFIN)
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withContentOffset(0)
            .withContentSizeInBytes(50)
            .withPath("deletes.puffin")
            .withReferencedDataFile(file.location())
            .build();

    table.newRowDelta().addDeletes(deletes).commit();

    // Delete file records do not count toward added rows
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.currentSnapshot().addedRows()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);
  }

  @TestTemplate
  public void testEqualityDeletes() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    DataFile file = fileWithRows(30);

    table.newAppend().appendFile(file).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    DeleteFile deletes =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofEqualityDeletes(table.schema().findField("x").fieldId())
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .withPath("deletes.parquet")
            .withReferencedDataFile(file.location())
            .build();

    table.newRowDelta().addDeletes(deletes).commit();

    // Delete file records do not count toward added rows
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.currentSnapshot().addedRows()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);
  }

  @TestTemplate
  public void testReplace() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    DataFile filePart1 = fileWithRows(30);
    DataFile filePart2 = fileWithRows(30);
    DataFile fileCompacted = fileWithRows(60);

    table.newAppend().appendFile(filePart1).appendFile(filePart2).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.currentSnapshot().addedRows()).isEqualTo(60);
    assertThat(table.ops().current().nextRowId()).isEqualTo(60);

    table.newRewrite().deleteFile(filePart1).deleteFile(filePart2).addFile(fileCompacted).commit();

    // rewrites produce new manifests without first-row-id or any information about how many rows
    // are new. without tracking a new metric for a manifest (e.g., assigned-rows) or assuming that
    // rewrites do not assign any new IDs, replace will allocate ranges like normal writes.
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(60);
    assertThat(table.currentSnapshot().addedRows()).isEqualTo(60);
    assertThat(table.ops().current().nextRowId()).isEqualTo(120);
  }

  @TestTemplate
  public void testMetadataRewrite() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    DataFile file1 = fileWithRows(30);
    DataFile file2 = fileWithRows(30);

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.currentSnapshot().addedRows()).isEqualTo(60);
    assertThat(table.ops().current().nextRowId()).isEqualTo(60);

    table.rewriteManifests().commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(60);
    assertThat(table.currentSnapshot().addedRows()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(60);
  }

  @TestTemplate
  public void testCommitTimestampOnAppend() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    table.newAppend().appendFile(fileWithRows(30)).commit();
    Snapshot snap = table.currentSnapshot();
    ManifestFile manifest = Iterables.getOnlyElement(snap.dataManifests(table.io()));

    if (formatVersion >= 4) {
      assertThat(manifest.commitTimestampMs())
          .as("V4 manifest should have commit_timestamp_ms matching snapshot timestamp")
          .isEqualTo(snap.timestampMillis());
      checkEntryCommitTimestamp(table, manifest, snap.timestampMillis());
    } else {
      assertThat(manifest.commitTimestampMs())
          .as("Pre-V4 manifest should have null commit_timestamp_ms")
          .isNull();
      checkEntryCommitTimestamp(table, manifest, null);
    }
  }

  @TestTemplate
  public void testCommitTimestampOnMultipleAppends() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    table.newAppend().appendFile(fileWithRows(30)).commit();
    long firstTimestamp = table.currentSnapshot().timestampMillis();

    table.newAppend().appendFile(fileWithRows(20)).commit();
    Snapshot second = table.currentSnapshot();
    long secondTimestamp = second.timestampMillis();

    List<ManifestFile> manifests = second.dataManifests(table.io());
    assertThat(manifests).hasSize(2);

    if (formatVersion >= 4) {
      assertThat(secondTimestamp).isGreaterThan(firstTimestamp);
      assertThat(manifests.get(0).commitTimestampMs()).isEqualTo(secondTimestamp);
      assertThat(manifests.get(1).commitTimestampMs()).isEqualTo(firstTimestamp);
      checkEntryCommitTimestamp(table, manifests.get(0), secondTimestamp);
      checkEntryCommitTimestamp(table, manifests.get(1), firstTimestamp);
    } else {
      assertThat(manifests.get(0).commitTimestampMs()).isNull();
      assertThat(manifests.get(1).commitTimestampMs()).isNull();
    }
  }

  @TestTemplate
  public void testCommitTimestampOnRewrite() {
    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    DataFile filePart1 = fileWithRows(30);
    DataFile filePart2 = fileWithRows(30);
    DataFile fileCompacted = fileWithRows(60);

    table.newAppend().appendFile(filePart1).appendFile(filePart2).commit();
    table.newRewrite().deleteFile(filePart1).deleteFile(filePart2).addFile(fileCompacted).commit();

    Snapshot rewrite = table.currentSnapshot();
    List<ManifestFile> manifests = rewrite.dataManifests(table.io());

    if (formatVersion >= 4) {
      for (ManifestFile manifest : manifests) {
        assertThat(manifest.commitTimestampMs())
            .as("Rewrite manifest should have commit_timestamp_ms")
            .isEqualTo(rewrite.timestampMillis());
      }
    } else {
      for (ManifestFile manifest : manifests) {
        assertThat(manifest.commitTimestampMs()).isNull();
      }
    }
  }

  private void checkEntryCommitTimestamp(
      Table table, ManifestFile manifest, Long expectedTimestamp) {
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, table.io(), table.specs())) {
      for (DataFile file : reader) {
        assertThat(file.commitTimestampMs())
            .as("Data file commit_timestamp_ms should match expected value")
            .isEqualTo(expectedTimestamp);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private final AtomicInteger fileNum = new AtomicInteger(0);

  private DataFile fileWithRows(long numRows) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(numRows)
        .withFileSizeInBytes(numRows * 100)
        .withPath("file://file_" + fileNum.incrementAndGet() + ".parquet")
        .build();
  }
}
