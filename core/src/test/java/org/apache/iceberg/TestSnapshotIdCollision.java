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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests a snapshot producer whose cached snapshot ID is taken by a concurrent commit.
 *
 * <p>A producer generates its snapshot ID once, checks it only against the table metadata it has at
 * that moment, and reuses it on every commit attempt. Each test stages a producer with {@code
 * apply()}, which caches the ID and writes its manifests, then lets a concurrent writer commit a
 * snapshot with the same ID before the staged producer commits. Two operations are covered:
 *
 * <ul>
 *   <li>Append: the appended data file is not in the table, and the staged manifest is left behind
 *       unreferenced because {@code FastAppend} skips cleanup after a successful commit.
 *   <li>Row delta: the delete file is not in the table, and the staged delete manifest is removed
 *       by cleanup.
 * </ul>
 *
 * <p>In both cases {@code commit()} returns normally. On the commit attempt, {@code
 * SnapshotProducer.commit()} finds a snapshot with its own ID in the refreshed metadata and treats
 * the commit as a rollback to that snapshot:
 *
 * <pre>{@code
 * if (base.snapshot(newSnapshot.snapshotId()) != null) {
 *   // this is a rollback operation
 *   update.setBranchSnapshot(newSnapshot.snapshotId(), targetBranch);
 * }
 * }</pre>
 *
 * <p>The branch already points at the concurrent snapshot, so the update has no changes and the
 * commit returns without error:
 *
 * <pre>{@code
 * TableMetadata updated = update.build();
 * if (updated.changes().isEmpty()) {
 *   // do not commit if the metadata has not changed. ...
 *   return;
 * }
 * }</pre>
 */
@ExtendWith(ParameterizedTestExtension.class)
class TestSnapshotIdCollision extends TestBase {

  @TestTemplate
  void appendWithSnapshotIdTakenByConcurrentCommit() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot base = table.currentSnapshot();

    AppendFiles append = table.newFastAppend().appendFile(FILE_B);
    Snapshot staged = append.apply();
    Set<String> stagedManifests = newManifestPaths(staged, base);

    Table concurrentTable = load();
    concurrentTable.newFastAppend().appendFile(FILE_C).commit();
    Snapshot concurrent = concurrentTable.currentSnapshot();
    // each TestTableOperations instance assigns sequential IDs starting after the table's highest
    // snapshot ID, so the newly loaded table reuses the ID already cached by the staged producer
    assertThat(concurrent.snapshotId()).isEqualTo(staged.snapshotId());

    append.commit();

    table.refresh();
    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.currentSnapshot().manifestListLocation())
        .isEqualTo(concurrent.manifestListLocation());
    validateTableFiles(table, FILE_A, FILE_C);
    assertThat(stagedManifests).isNotEmpty().allMatch(path -> new File(path).exists());
  }

  @TestTemplate
  void rowDeltaWithSnapshotIdTakenByConcurrentCommit() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot base = table.currentSnapshot();

    RowDelta rowDelta = table.newRowDelta().addDeletes(fileADeletes());
    Snapshot staged = rowDelta.apply();
    Set<String> stagedManifests = newManifestPaths(staged, base);

    Table concurrentTable = load();
    concurrentTable.newFastAppend().appendFile(FILE_B).commit();
    Snapshot concurrent = concurrentTable.currentSnapshot();
    // each TestTableOperations instance assigns sequential IDs starting after the table's highest
    // snapshot ID, so the newly loaded table reuses the ID already cached by the staged producer
    assertThat(concurrent.snapshotId()).isEqualTo(staged.snapshotId());

    rowDelta.commit();

    table.refresh();
    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.currentSnapshot().manifestListLocation())
        .isEqualTo(concurrent.manifestListLocation());
    assertThat(table.currentSnapshot().deleteManifests(table.io())).isEmpty();
    assertThat(stagedManifests).isNotEmpty().noneMatch(path -> new File(path).exists());
  }

  private Set<String> newManifestPaths(Snapshot snapshot, Snapshot parent) {
    Set<String> parentManifests =
        parent.allManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toSet());
    return snapshot.allManifests(table.io()).stream()
        .map(ManifestFile::path)
        .filter(path -> !parentManifests.contains(path))
        .collect(Collectors.toSet());
  }
}
