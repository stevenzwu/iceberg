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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.jupiter.api.TestTemplate;

public class TestTimeTravel extends TestBaseWithCatalog {

    @TestTemplate
    public void testTravelAfterRollback() {
        // create a v1 table
        sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg tblproperties ('format-version'='2')", tableName);

        Table table = validationCatalog.loadTable(tableIdent);

        // append some rows
        sql("INSERT INTO %s VALUES (1, 'a')", tableName);
        table.refresh();
        assertThat(table.snapshots()).hasSize(1);
        Snapshot snapshot1 = table.currentSnapshot();

        waitUntilAfter(snapshot1.timestampMillis() + 1000);

        sql("INSERT INTO %s VALUES (2, 'b')", tableName);
        table.refresh();
        assertThat(table.snapshots()).hasSize(2);
        Snapshot snapshot2 = table.currentSnapshot();

        waitUntilAfter(snapshot2.timestampMillis() + 1000);

        sql("INSERT INTO %s VALUES (3, 'c')", tableName);
        table.refresh();
        assertThat(table.snapshots()).hasSize(3);
        Snapshot snapshot3 = table.currentSnapshot();

        waitUntilAfter(snapshot3.timestampMillis() + 1000);

        // AS OF expects the timestamp if given in long format will be of seconds precision
        long afterSnapshot2TimestampInSeconds = TimeUnit.MILLISECONDS.toSeconds(snapshot2.timestampMillis() + 1000);

        // time travel to snapshot2
        assertThat(sql("SELECT * FROM %s TIMESTAMP AS OF %d", tableName, afterSnapshot2TimestampInSeconds))
            .hasSize(2)
            .containsExactlyInAnyOrder(row(1L, "a"), row(2L, "b"));

        // rollback to snapshot1
        table.manageSnapshots().rollbackTo(snapshot1.snapshotId()).commit();
        table.refresh();
        Snapshot snapshotAfterRollback = table.currentSnapshot();
        assertThat(snapshotAfterRollback.snapshotId()).isEqualTo(snapshot1.snapshotId());
        // snapshots list should still contain all 3 snapshots
        assertThat(table.snapshots()).hasSize(3);
        // but only snapshot1 should be reachable
        assertThat(SnapshotUtil.currentAncestors(table)).containsExactly(snapshot1);

        // verify data at current snapshot
        assertThat(sql("SELECT * FROM %s", tableName))
                .hasSize(1)
                .containsExactlyInAnyOrder(row(1L, "a"));

        // append new data after rollback
        sql("INSERT INTO %s VALUES (4, 'd')", tableName);
        table.refresh();
        assertThat(table.snapshots()).hasSize(4);
        Snapshot snapshot4 = table.currentSnapshot();

        // verify data at current snapshot
        assertThat(sql("SELECT * FROM %s", tableName))
                .hasSize(2)
                .containsExactlyInAnyOrder(row(1L, "a"), row(4L, "d"));

        // time travel to snapshot2
        assertThat(sql("SELECT * FROM %s TIMESTAMP AS OF %d", tableName, afterSnapshot2TimestampInSeconds))
                .hasSize(2)
                .containsExactlyInAnyOrder(row(1L, "a"), row(2L, "b"));
    }

}

