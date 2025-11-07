/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.spark.sql;

import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.junit.jupiter.api.TestTemplate;

import static org.apache.iceberg.DataOperations.DELETE;
import static org.apache.iceberg.DataOperations.OVERWRITE;
import static org.apache.iceberg.SnapshotSummary.ADDED_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_DVS_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADD_POS_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_COUNT_PROP;
import static org.apache.iceberg.SnapshotSummary.DELETED_FILES_PROP;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTableVersionUpgrade extends TestBaseWithCatalog {

    @TestTemplate
    public void testV1ToV3Upgrade() {
        // create a v1 table
        sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg tblproperties ('format-version'='1')", tableName);

        // append some rows to the v1 table
        sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

        Table table = validationCatalog.loadTable(tableIdent);
        assertThat(table.snapshots()).hasSize(1);
        Snapshot currentSnapshot = table.currentSnapshot();
        assertThat(((BaseTable) table).operations().current().formatVersion()).isEqualTo(1);

        // upgrade the table to v3
        sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='3')", tableName);

        // insert more rows to the upgraded v3 table
        sql("INSERT INTO %s VALUES (4, 'd'), (5, 'e')", tableName);

        // set MoR mode
        sql("ALTER TABLE %s SET TBLPROPERTIES ('write.delete.mode'='merge-on-read')", tableName);

        // perform some row level deletes in MoR style
        sql("DELETE FROM %s WHERE id = 2", tableName);

        table.refresh();
        assertThat(((BaseTable) table).operations().current().formatVersion()).isEqualTo(3);
        assertThat(table.snapshots()).hasSize(3);
        currentSnapshot = table.currentSnapshot();
        validateMergeOnRead(currentSnapshot, "1", "1", null);

        // verify row lineage
        sql("SELECT _row_id, _last_updated_sequence_number, id FROM %s", tableName);
    }

    protected void validateMergeOnRead(
            Snapshot snapshot,
            String changedPartitionCount,
            String addedDeleteFiles,
            String addedDataFiles) {
        String operation = null == addedDataFiles && null != addedDeleteFiles ? DELETE : OVERWRITE;
        validateSnapshot(
                snapshot, operation, changedPartitionCount, null, addedDeleteFiles, addedDataFiles);
    }

    protected void validateSnapshot(
            Snapshot snapshot,
            String operation,
            String changedPartitionCount,
            String deletedDataFiles,
            String addedDeleteFiles,
            String addedDataFiles) {
        assertThat(snapshot.operation()).as("Operation must match").isEqualTo(operation);
        validateProperty(snapshot, CHANGED_PARTITION_COUNT_PROP, changedPartitionCount);
        validateProperty(snapshot, DELETED_FILES_PROP, deletedDataFiles);
        validateProperty(snapshot, ADDED_DELETE_FILES_PROP, addedDeleteFiles);
        validateProperty(snapshot, ADDED_FILES_PROP, addedDataFiles);
        validateProperty(snapshot, ADDED_DVS_PROP, addedDeleteFiles);
        assertThat(snapshot.summary()).doesNotContainKey(ADD_POS_DELETE_FILES_PROP);
    }

    protected void validateProperty(Snapshot snapshot, String property, Set<String> expectedValues) {
        String actual = snapshot.summary().get(property);
        assertThat(actual)
                .as(
                        "Snapshot property "
                                + property
                                + " has unexpected value, actual = "
                                + actual
                                + ", expected one of : "
                                + String.join(",", expectedValues))
                .isIn(expectedValues);
    }

    protected void validateProperty(Snapshot snapshot, String property, String expectedValue) {
        if (null == expectedValue) {
            assertThat(snapshot.summary()).doesNotContainKey(property);
        } else {
            assertThat(snapshot.summary())
                    .as("Snapshot property " + property + " has unexpected value.")
                    .containsEntry(property, expectedValue);
        }
    }
}
