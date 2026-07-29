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
import java.util.Map;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Scan-side coverage for v4 colocated deletion vectors.
 *
 * <p>Phase 5 collapsed the two-phase scan planning that used to extract DVs from data manifests and
 * re-index them by {@code referencedDataFile()} in {@link DeleteFileIndex}. Each v4 DATA manifest
 * is now opened once; DVs flow inline with the {@link ManifestEntry} that carries their host data
 * file, and {@link ManifestGroup} attaches them directly to the resulting {@link
 * FileScanTask#deletes()} array.
 */
public class TestV4ScanColocatedDV {

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

  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(100)
          .withPartitionPath("data_bucket=1")
          .withRecordCount(5)
          .build();

  @TempDir File tableDir;

  private TestTables.TestTable table;

  @BeforeEach
  public void before() {
    table = TestTables.create(tableDir, tableDir.getName(), SCHEMA, SPEC, SortOrder.unsorted(), 4);
  }

  /**
   * Baseline: a data file with a colocated DV must surface the DV on the scan task's {@code
   * deletes()} array. No {@link DeleteFileIndex} is populated for this snapshot — the DV flows
   * inline from the same content_entry row that produced the data file.
   */
  @Test
  public void testPlanFilesAttachesColocatedDV() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();

    List<FileScanTask> tasks = planFiles();
    assertThat(tasks).as("scan must surface exactly one data file").hasSize(1);

    FileScanTask task = tasks.get(0);
    assertThat(task.file().location()).isEqualTo(FILE_A.location());
    assertThat(task.deletes()).as("colocated DV must appear on task.deletes()").hasSize(1);

    DeleteFile scanDV = task.deletes().get(0);
    assertThat(scanDV.content()).isEqualTo(FileContent.POSITION_DELETES);
    assertThat(scanDV.format()).isEqualTo(FileFormat.PUFFIN);
    assertThat(ContentFileUtil.isDV(scanDV)).isTrue();
    assertThat(scanDV.referencedDataFile()).isEqualTo(FILE_A.location());
    assertThat(scanDV.location()).isEqualTo(dv.location());
  }

  /**
   * Mixed manifest: two data files in the same v4 leaf, only one with a DV. Each scan task must
   * carry the correct {@code deletes()} array — one singleton, one empty — proving DV attachment
   * happens per-entry rather than per-manifest.
   */
  @Test
  public void testPlanFilesMixedDataFilesInSameManifest() throws IOException {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addDeletes(dv).commit();

    List<FileScanTask> tasks = planFiles();
    assertThat(tasks).as("scan must surface both data files").hasSize(2);

    Map<String, FileScanTask> byPath = Maps.newHashMap();
    for (FileScanTask task : tasks) {
      byPath.put(task.file().location(), task);
    }

    FileScanTask taskA = byPath.get(FILE_A.location());
    assertThat(taskA).as("FILE_A task must be present").isNotNull();
    assertThat(taskA.deletes()).as("FILE_A must carry its colocated DV").hasSize(1);
    assertThat(taskA.deletes().get(0).referencedDataFile()).isEqualTo(FILE_A.location());
    assertThat(taskA.deletes().get(0).location()).isEqualTo(dv.location());

    FileScanTask taskB = byPath.get(FILE_B.location());
    assertThat(taskB).as("FILE_B task must be present").isNotNull();
    assertThat(taskB.deletes()).as("FILE_B has no DV; deletes() must be empty").isEmpty();
  }

  /**
   * Direct-row path: a small commit lands as a direct row in the root manifest (adaptive-tree
   * small-write optimization) rather than as an entry in a real leaf. A born-with-DV commit routes
   * the ADDED row + DV into that same direct-row surface. Scan planning must still attach the DV to
   * the task, exercising the shared {@link V4ManifestReader#directDataEntriesFromRoot()} decode
   * path.
   */
  @Test
  public void testPlanFilesDirectRowWithColocatedDV() throws IOException {
    // Force adaptive spill so the accumulator promotes small writes to root direct rows instead
    // of packing them into a real leaf.
    table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "1000").commit();

    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newRowDelta().addRows(FILE_A).addDeletes(dv).commit();

    List<FileScanTask> tasks = planFiles();
    assertThat(tasks).as("scan must surface FILE_A from a direct-row commit").hasSize(1);

    FileScanTask task = tasks.get(0);
    assertThat(task.file().location()).isEqualTo(FILE_A.location());
    assertThat(task.deletes())
        .as("born-with-DV must attach through direct-row iteration")
        .hasSize(1);
    assertThat(task.deletes().get(0).referencedDataFile()).isEqualTo(FILE_A.location());
    assertThat(task.deletes().get(0).location()).isEqualTo(dv.location());
  }

  /**
   * FastAppend single-call born-with-DV: {@link AppendFiles#appendFile(DataFile, DeleteFile)} on
   * {@code table.newAppend()} routes the DV through {@code FastAppend.bornWithDVByPath} into {@code
   * SnapshotProducer.injectV4AdaptiveDataFiles}. Scan planning must surface the DV on the task,
   * matching the RowDelta-born and chained variants.
   */
  @Test
  public void testPlanFilesFastAppendBornWithDV() throws IOException {
    DeleteFile dv = FileGenerationUtil.generateDV(table, FILE_A);
    table.newAppend().appendFile(FILE_A, dv).commit();

    List<FileScanTask> tasks = planFiles();
    assertThat(tasks)
        .as("scan must surface FILE_A from a FastAppend born-with-DV commit")
        .hasSize(1);

    FileScanTask task = tasks.get(0);
    assertThat(task.file().location()).isEqualTo(FILE_A.location());
    assertThat(task.deletes())
        .as("FastAppend born-with-DV must attach through inline entry DV")
        .hasSize(1);
    assertThat(task.deletes().get(0).referencedDataFile()).isEqualTo(FILE_A.location());
    assertThat(task.deletes().get(0).location()).isEqualTo(dv.location());
  }

  private List<FileScanTask> planFiles() throws IOException {
    List<FileScanTask> tasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> iter = table.newScan().planFiles()) {
      for (FileScanTask task : iter) {
        tasks.add(task);
      }
    }
    return tasks;
  }
}
