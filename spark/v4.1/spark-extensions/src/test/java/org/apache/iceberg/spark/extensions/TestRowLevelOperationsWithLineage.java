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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.MetadataColumns.schemaWithRowLineage;
import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_HASH;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE_RANGE;
import static org.apache.iceberg.spark.Spark3Util.loadIcebergTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.functions.BucketFunction;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;

public abstract class TestRowLevelOperationsWithLineage extends SparkRowLevelOperationsTestBase {
  static final Function<StructLike, StructLike> BUCKET_PARTITION_GENERATOR =
      record ->
          TestHelpers.Row.of(BucketFunction.BucketInt.invoke(2, record.get(0, Integer.class)));

  static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.IntegerType.get()),
              Types.NestedField.required(2, "data", Types.StringType.get()),
              MetadataColumns.ROW_ID,
              MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER,
              MetadataColumns.LAST_UPDATED_TIMESTAMP_MS));

  // The records below simulate freshly inserted rows: _last_updated_timestamp_ms is left null so
  // that on read it inherits commit_timestamp_ms from the manifest entry (the V4 contract). The
  // _row_id and _last_updated_sequence_number fields are still populated explicitly because the
  // tests rely on specific values for those columns; the readers always prefer a non-null row-level
  // value, so the literal values flow through unchanged.
  static final List<Record> INITIAL_RECORDS =
      ImmutableList.of(
          createRecord(SCHEMA, 100, "a", 0L, 1L, null),
          createRecord(SCHEMA, 101, "b", 1L, 1L, null),
          createRecord(SCHEMA, 102, "c", 2L, 1L, null),
          createRecord(SCHEMA, 103, "d", 3L, 1L, null),
          createRecord(SCHEMA, 104, "e", 4L, 1L, null));

  @Parameters(
      name =
          "catalogName = {0}, implementation = {1}, config = {2},"
              + " format = {3}, vectorized = {4}, distributionMode = {5},"
              + " fanout = {6}, branch = {7}, planningMode = {8}, formatVersion = {9}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of("type", "hadoop"),
        FileFormat.PARQUET,
        true,
        WRITE_DISTRIBUTION_MODE_HASH,
        true,
        null,
        LOCAL,
        3
      },
      {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of("type", "hadoop"),
        FileFormat.PARQUET,
        false,
        WRITE_DISTRIBUTION_MODE_RANGE,
        true,
        null,
        DISTRIBUTED,
        3
      },
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type",
            "hive",
            "default-namespace",
            "default",
            "clients",
            "1",
            "parquet-enabled",
            "false",
            "cache-enabled",
            "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
        FileFormat.AVRO,
        false,
        WRITE_DISTRIBUTION_MODE_RANGE,
        false,
        null,
        DISTRIBUTED,
        3
      },
      {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of("type", "hadoop"),
        FileFormat.PARQUET,
        true,
        WRITE_DISTRIBUTION_MODE_HASH,
        true,
        null,
        LOCAL,
        4
      },
    };
  }

  @BeforeAll
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  @TestTemplate
  public void testMergeIntoWithBothMatchedAndNonMatched()
      throws NoSuchTableException, ParseException, IOException {
    createAndInitTable("id INT, data STRING", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    appendUnpartitionedRecords(table, INITIAL_RECORDS);
    createOrReplaceView(
        "source",
        "id int, data string",
        "{ \"id\": 101, \"data\": \"updated_b\" }\n " + "{ \"id\": 200, \"data\": \"f\" }\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = s.data "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    Snapshot updateSnapshot = latestSnapshot(table);
    long updateSnapshotFirstRowId = updateSnapshot.firstRowId();
    List<Object[]> allRows = rowsWithLineageAndFilePos();
    List<Object[]> carriedOverAndUpdatedRows =
        allRows.stream()
            .filter(row -> (long) row[3] < updateSnapshotFirstRowId)
            .collect(Collectors.toList());

    // Project sequence numbers first for easier comparison on the added row
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(1L, 100, "a", 0L, ANY, ANY),
            row(updateSnapshot.sequenceNumber(), 101, "updated_b", 1L, ANY, ANY),
            row(1L, 102, "c", 2L, ANY, ANY),
            row(1L, 103, "d", 3L, ANY, ANY),
            row(1L, 104, "e", 4L, ANY, ANY)),
        carriedOverAndUpdatedRows);

    Object[] newRow =
        Iterables.getOnlyElement(
            allRows.stream()
                .filter(row -> (long) row[3] >= updateSnapshotFirstRowId)
                .collect(Collectors.toList()));
    assertAddedRowLineage(row(updateSnapshot.sequenceNumber(), 200, "f"), newRow);
  }

  @TestTemplate
  public void testMergeIntoWithBothMatchedAndNonMatchedPartitioned()
      throws NoSuchTableException, ParseException, IOException {
    createAndInitTable("id INT, data STRING", "PARTITIONED BY (bucket(2, id))", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    appendRecords(
        table, partitionRecords(INITIAL_RECORDS, table.spec(), BUCKET_PARTITION_GENERATOR));
    createOrReplaceView(
        "source",
        "id int, data string",
        "{ \"id\": 101, \"data\": \"updated_b\" }\n " + "{ \"id\": 200, \"data\": \"f\" }\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = s.data "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    Snapshot updateSnapshot = latestSnapshot(table);
    long updateSnapshotFirstRowId = updateSnapshot.firstRowId();
    List<Object[]> allRows = rowsWithLineageAndFilePos();

    List<Object[]> carriedOverAndUpdatedRows =
        allRows.stream()
            .filter(row -> (long) row[3] < updateSnapshotFirstRowId)
            .collect(Collectors.toList());

    // Project sequence numbers first for easier comparison on the added row
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(1L, 100, "a", 0L, ANY, ANY),
            row(updateSnapshot.sequenceNumber(), 101, "updated_b", 1L, ANY, ANY),
            row(1L, 102, "c", 2L, ANY, ANY),
            row(1L, 103, "d", 3L, ANY, ANY),
            row(1L, 104, "e", 4L, ANY, ANY)),
        carriedOverAndUpdatedRows);

    Object[] newRow =
        Iterables.getOnlyElement(
            allRows.stream()
                .filter(row -> (long) row[3] >= updateSnapshotFirstRowId)
                .collect(Collectors.toList()));
    assertAddedRowLineage(row(updateSnapshot.sequenceNumber(), 200, "f"), newRow);
  }

  @TestTemplate
  public void testMergeIntoWithOnlyNonMatched()
      throws NoSuchTableException, ParseException, IOException {
    createAndInitTable("id INT, data string", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    appendUnpartitionedRecords(table, INITIAL_RECORDS);
    createOrReplaceView(
        "source",
        "id INT, data STRING",
        "{ \"id\": 101, \"data\": \"updated_b\" }\n " + "{ \"id\": 200, \"data\": \"f\" }\n");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN NOT MATCHED THEN "
            + "INSERT *",
        commitTarget());

    Snapshot updateSnapshot = latestSnapshot(table);
    long updateSnapshotFirstRowId = updateSnapshot.firstRowId();

    List<Object[]> allRows = rowsWithLineageAndFilePos();
    List<Object[]> carriedOverAndUpdatedRows =
        allRows.stream()
            .filter(row -> (long) row[3] < updateSnapshotFirstRowId)
            .collect(Collectors.toList());

    // Project sequence numbers first for easier comparison on the added row
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(1L, 100, "a", 0L, ANY, ANY),
            row(1L, 101, "b", 1L, ANY, ANY),
            row(1L, 102, "c", 2L, ANY, ANY),
            row(1L, 103, "d", 3L, ANY, ANY),
            row(1L, 104, "e", 4L, ANY, ANY)),
        carriedOverAndUpdatedRows);

    Object[] newRow =
        Iterables.getOnlyElement(
            allRows.stream()
                .filter(row -> (long) row[3] >= updateSnapshotFirstRowId)
                .collect(Collectors.toList()));
    assertAddedRowLineage(row(updateSnapshot.sequenceNumber(), 200, "f"), newRow);
  }

  @TestTemplate
  public void testMergeIntoWithOnlyMatched()
      throws IOException, NoSuchTableException, ParseException {
    createAndInitTable("id INT, data STRING", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    appendUnpartitionedRecords(table, INITIAL_RECORDS);
    long appendTimestamp = latestSnapshot(table).timestampMillis();
    createOrReplaceView(
        "source",
        "id INT, data string",
        "{ \"id\": 101, \"data\": \"updated_b\" }\n "
            + "{ \"id\": 102, \"data\": \"updated_c\" }\n");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = s.data ",
        commitTarget());

    Snapshot updateSnapshot = latestSnapshot(table);
    long updateSequenceNumber = updateSnapshot.sequenceNumber();
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(100, "a", 0L, 1L),
            row(101, "updated_b", 1L, updateSequenceNumber),
            row(102, "updated_c", 2L, updateSequenceNumber),
            row(103, "d", 3L, 1L),
            row(104, "e", 4L, 1L)),
        rowsWithLineage());

    if (formatVersion >= 4) {
      long updateTimestamp = updateSnapshot.timestampMillis();
      assertTimestamps(
          ImmutableList.of(
              row(0L, appendTimestamp),
              row(1L, updateTimestamp),
              row(2L, updateTimestamp),
              row(3L, appendTimestamp),
              row(4L, appendTimestamp)));
    } else {
      assertAllTimestampsNull();
    }
  }

  @TestTemplate
  public void testMergeMatchedDelete() throws NoSuchTableException, ParseException, IOException {
    createAndInitTable("id INT, data STRING", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    appendUnpartitionedRecords(table, INITIAL_RECORDS);
    createOrReplaceView(
        "source",
        "id INT, data string",
        "{ \"id\": 101, \"data\": \"delete_101\" }\n "
            + "{ \"id\": 102, \"data\": \"delete_102\" }\n");
    sql(
        "MERGE INTO %s AS t USING source AS s " + "ON t.id == s.id " + "WHEN MATCHED THEN DELETE",
        commitTarget());

    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(row(100, "a", 0L, 1L), row(103, "d", 3L, 1L), row(104, "e", 4L, 1L)),
        rowsWithLineage());
  }

  @TestTemplate
  public void testMergeWhenNotMatchedBySource()
      throws NoSuchTableException, ParseException, IOException {
    createAndInitTable("id INT, data STRING", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    appendUnpartitionedRecords(table, INITIAL_RECORDS);
    createOrReplaceView(
        "source",
        "id INT, data STRING",
        "{ \"id\": 101, \"data\": \"updated_b\" }\n " + "{ \"id\": 200, \"data\": \"f\" }\n");

    sql(
        "MERGE INTO %s AS t USING source AS s ON t.id == s.id"
            + " WHEN MATCHED THEN UPDATE set t.data = s.data "
            + "WHEN NOT MATCHED BY SOURCE THEN UPDATE set data = 'not_matched_by_source'",
        commitTarget());

    long updateSequenceNumber = latestSnapshot(table).sequenceNumber();

    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(100, "not_matched_by_source", 0L, updateSequenceNumber),
            row(101, "updated_b", 1L, updateSequenceNumber),
            row(102, "not_matched_by_source", 2L, updateSequenceNumber),
            row(103, "not_matched_by_source", 3L, updateSequenceNumber),
            row(104, "not_matched_by_source", 4L, updateSequenceNumber)),
        rowsWithLineage());
  }

  @TestTemplate
  public void testMergeWhenNotMatchedBySourceDelete()
      throws NoSuchTableException, ParseException, IOException {
    createAndInitTable("id INT, data STRING", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    appendUnpartitionedRecords(table, INITIAL_RECORDS);
    createOrReplaceView(
        "source",
        "id INT, data STRING",
        "{ \"id\": 101, \"data\": \"updated_b\" }\n "
            + "{ \"id\": 102, \"data\": \"updated_c\" }\n");

    sql(
        "MERGE INTO %s AS t USING source AS s ON t.id == s.id"
            + " WHEN MATCHED THEN UPDATE set t.data = s.data "
            + "WHEN NOT MATCHED BY SOURCE THEN DELETE",
        commitTarget());

    long updateSequenceNumber = latestSnapshot(table).sequenceNumber();
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(101, "updated_b", 1L, updateSequenceNumber),
            row(102, "updated_c", 2L, updateSequenceNumber)),
        rowsWithLineage());
  }

  @TestTemplate
  public void testUpdate() throws NoSuchTableException, ParseException, IOException {
    createAndInitTable("id INT, data STRING", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    appendUnpartitionedRecords(table, INITIAL_RECORDS);
    long appendTimestamp = latestSnapshot(table).timestampMillis();

    sql("UPDATE %s AS t set data = 'updated_b' WHERE id = 101", commitTarget());
    Snapshot updateSnapshot = latestSnapshot(table);
    long updateSequenceNumber = updateSnapshot.sequenceNumber();

    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(100, "a", 0L, 1L),
            row(101, "updated_b", 1L, updateSequenceNumber),
            row(102, "c", 2L, 1L),
            row(103, "d", 3L, 1L),
            row(104, "e", 4L, 1L)),
        rowsWithLineage());

    if (formatVersion < 4) {
      assertAllTimestampsNull();
      return;
    }

    if (formatVersion >= 4) {
      long updateTimestamp = updateSnapshot.timestampMillis();
      assertTimestamps(
          ImmutableList.of(
              row(0L, appendTimestamp),
              row(1L, updateTimestamp),
              row(2L, appendTimestamp),
              row(3L, appendTimestamp),
              row(4L, appendTimestamp)));
    }
  }

  @TestTemplate
  public void testDelete() throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    createAndInitTable("id int, data STRING", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    appendUnpartitionedRecords(table, INITIAL_RECORDS);

    sql("DELETE FROM %s WHERE id = 101", commitTarget());

    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(100, "a", 0L, 1L),
            row(102, "c", 2L, 1L),
            row(103, "d", 3L, 1L),
            row(104, "e", 4L, 1L)),
        rowsWithLineage());
  }

  @TestTemplate
  public void testMergeWithManyRecords() throws NoSuchTableException, ParseException, IOException {
    createAndInitTable("id INT, data STRING", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);

    int numRecords = 25000;
    int startingId = 100;

    List<Record> initialRecords = Lists.newArrayList();
    int rowId = 0;
    for (int id = 100; id < startingId + numRecords; id++) {
      initialRecords.add(createRecord(SCHEMA, id, "data_" + id, rowId++, 1L, null));
    }

    appendUnpartitionedRecords(table, initialRecords);
    createOrReplaceView(
        "source",
        "id int, data string",
        "{ \"id\": 101, \"data\": \"updated_data_101\" }\n "
            + "{ \"id\": 26000, \"data\": \"data_26000\" }\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = s.data "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    Snapshot updateSnapshot = latestSnapshot(table);
    long updateSnapshotFirstRowId = updateSnapshot.firstRowId();
    List<Object[]> allRows = rowsWithLineageAndFilePos();
    List<Object[]> carriedOverAndUpdatedRows =
        allRows.stream()
            .filter(row -> (long) row[3] < updateSnapshotFirstRowId)
            .collect(Collectors.toList());

    int newlyInsertedId = 26000;
    int updatedId = 101;
    List<Object[]> expectedCarriedOverAndUpdatedRows =
        ImmutableList.<Object[]>builder()
            .add(row(1L, 100, "data_100", 0L, ANY, ANY))
            .add(row(updateSnapshot.sequenceNumber(), updatedId, "updated_data_101", 1L, ANY, ANY))
            .addAll(
                // Every record with higher ids than the updated excluding the new row should be a
                // carry over
                initialRecords.stream()
                    .filter(
                        initialRecord -> {
                          int id = initialRecord.get(0, Integer.class);
                          return id > updatedId && id != newlyInsertedId;
                        })
                    .map(this::recordToExpectedRow)
                    .collect(Collectors.toList()))
            .build();

    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        expectedCarriedOverAndUpdatedRows,
        carriedOverAndUpdatedRows);

    Object[] newRow =
        Iterables.getOnlyElement(
            allRows.stream()
                .filter(row -> (long) row[3] >= updateSnapshotFirstRowId)
                .collect(Collectors.toList()));
    assertAddedRowLineage(row(updateSnapshot.sequenceNumber(), 26000, "data_26000"), newRow);
  }

  private Object[] recordToExpectedRow(Record record) {
    int id = record.get(0, Integer.class);
    String data = record.get(1, String.class);
    long rowId = record.get(2, Long.class);
    long lastUpdated = record.get(3, Long.class);
    return row(lastUpdated, id, data, rowId, ANY, ANY);
  }

  private List<Object[]> rowsWithLineageAndFilePos() {
    return sql(
        "SELECT s._last_updated_sequence_number, s.id, s.data, s._row_id, files.first_row_id, s._pos FROM %s"
            + " AS s JOIN %s.files AS files ON files.file_path = s._file ORDER BY s._row_id",
        selectTarget(), selectTarget());
  }

  private List<Object[]> rowsWithLineage() {
    return sql(
        "SELECT id, data, _row_id, _last_updated_sequence_number FROM %s ORDER BY _row_id",
        selectTarget());
  }

  private void assertTimestamps(List<Object[]> expected) {
    List<Object[]> actual =
        sql("SELECT _row_id, _last_updated_timestamp_ms FROM %s ORDER BY _row_id", selectTarget());
    assertEquals("Rows should have expected timestamps", expected, actual);
  }

  /**
   * Verifies that every row in the table has a {@code null} {@code _last_updated_timestamp_ms}.
   * Used to assert the V3 (and earlier) contract: the metadata column is exposed but always null
   * because pre-V4 manifests do not record a commit timestamp.
   */
  private void assertAllTimestampsNull() {
    List<Object[]> actual = sql("SELECT _last_updated_timestamp_ms FROM %s", selectTarget());
    for (Object[] r : actual) {
      assertThat(r[0])
          .as("Pre-V4 tables must expose null _last_updated_timestamp_ms for every row")
          .isNull();
    }
  }

  @TestTemplate
  public void testV3ToV4UpgradeTimestampInheritance()
      throws NoSuchTableException, ParseException, IOException {
    // The base parameterization for this suite already covers V3 and V4 separately. Run the
    // upgrade scenario only when the table starts at V3 to avoid duplicating the assertion under
    // the V4 parameter (where the table is created at V4 and there is nothing to upgrade).
    assumeThat(formatVersion).isEqualTo(3);
    // The session catalog parameterization re-creates the table on each operation in a way that
    // makes the explicit upgrade contract under test below fragile. Restrict to the hadoop catalog.
    assumeThat(catalogName).isEqualTo("testhadoop");
    // The branch variant routes writes through createBranchIfNeeded(), which complicates the
    // pre-upgrade vs. post-upgrade snapshot bookkeeping. Restrict to the main branch so we can
    // assert against currentSnapshot() directly.
    assumeThat(branch).isNull();

    createAndInitTable("id INT, data STRING", null);
    Table table = loadIcebergTable(spark, tableName);
    appendUnpartitionedRecords(table, INITIAL_RECORDS);

    // Pre-upgrade: V3 manifests do not carry commit_timestamp_ms, so the metadata column must
    // read as null for every row regardless of what the data file holds.
    assertAllTimestampsNull();

    // Upgrade the table format to V4. New snapshots written from this point on must carry
    // commit_timestamp_ms; previously committed manifests continue to read as null because the
    // V3 manifests in the manifest list have no commit_timestamp_ms to inherit.
    sql("ALTER TABLE %s SET TBLPROPERTIES('format-version' '4')", tableName);
    table.refresh();
    assertThat(TableUtil.formatVersion(table)).as("Table should be upgraded to V4").isEqualTo(4);

    // Insert a new row via the production Spark write path post-upgrade. The new manifest is
    // written at V4 and carries commit_timestamp_ms; the reader must inherit that value for the
    // new row.
    sql("INSERT INTO %s VALUES (200, 'f')", tableName);
    table.refresh();
    long postUpgradeTimestamp = table.currentSnapshot().timestampMillis();

    // The pre-upgrade rows continue to read as null (their V3 manifests have no
    // commit_timestamp_ms). The post-upgrade row inherits the new V4 snapshot's
    // commit_timestamp_ms.
    List<Object[]> actual =
        sql("SELECT id, _last_updated_timestamp_ms FROM %s ORDER BY id", selectTarget());
    assertEquals(
        "Pre-upgrade rows must read null; post-upgrade row must inherit V4 commit_timestamp_ms",
        ImmutableList.of(
            row(100, null),
            row(101, null),
            row(102, null),
            row(103, null),
            row(104, null),
            row(200, postUpgradeTimestamp)),
        actual);
  }

  /**
   * Partitions the provided records based on the spec and partition function
   *
   * @return a partitioned map
   */
  protected PartitionMap<List<Record>> partitionRecords(
      List<Record> records,
      PartitionSpec spec,
      Function<StructLike, StructLike> partitionGenerator) {
    PartitionMap<List<Record>> recordsByPartition =
        PartitionMap.create(Map.of(spec.specId(), spec));
    for (Record record : records) {
      StructLike partition = partitionGenerator != null ? partitionGenerator.apply(record) : null;
      List<Record> recordsForPartition = recordsByPartition.get(spec.specId(), partition);
      if (recordsForPartition == null) {
        recordsForPartition = Lists.newArrayList();
      }

      recordsForPartition.add(record);
      recordsByPartition.put(spec.specId(), partition, recordsForPartition);
    }

    return recordsByPartition;
  }

  protected void appendUnpartitionedRecords(Table table, List<Record> records) throws IOException {
    appendRecords(table, partitionRecords(records, table.spec(), record -> null));
  }

  protected void appendRecords(Table table, PartitionMap<List<Record>> partitionedRecords)
      throws IOException {
    AppendFiles append = table.newAppend();

    for (Map.Entry<Pair<Integer, StructLike>, List<Record>> entry : partitionedRecords.entrySet()) {
      OutputFile file = Files.localOutput(temp.resolve(UUID.randomUUID().toString()).toFile());
      DataWriter<Record> writer =
          new GenericFileWriterFactory.Builder(table)
              .dataSchema(schemaWithRowLineage(table.schema()))
              .dataFileFormat(fileFormat)
              .build()
              .newDataWriter(
                  EncryptionUtil.plainAsEncryptedOutput(file),
                  table.spec(),
                  entry.getKey().second());
      List<Record> recordsForPartition = entry.getValue();
      writer.write(recordsForPartition);
      writer.close();
      append =
          append
              .appendFile(writer.toDataFile())
              .toBranch(branch != null ? branch : SnapshotRef.MAIN_BRANCH);
    }

    append.commit();
  }

  protected static Record createRecord(
      Schema schema,
      int id,
      String data,
      long rowId,
      long lastUpdatedSequenceNumber,
      Long lastUpdatedTimestampMs) {
    Record record = GenericRecord.create(schema);
    record.set(0, id);
    record.set(1, data);
    record.set(2, rowId);
    record.set(3, lastUpdatedSequenceNumber);
    record.set(4, lastUpdatedTimestampMs);
    return record;
  }

  private Snapshot latestSnapshot(Table table) {
    table.refresh();
    return branch != null ? table.snapshot(branch) : table.currentSnapshot();
  }

  // Expected should have last updated sequence number followed by data columns
  // Actual should have the contents of expected followed by the file first row ID and position
  private void assertAddedRowLineage(Object[] expected, Object[] actual) {
    // validate the sequence number and all the data columns
    for (int pos = 0; pos < expected.length; pos++) {
      assertThat(actual[pos]).isEqualTo(expected[pos]);
    }

    int rowIdPos = expected.length;
    int firstRowIdPos = rowIdPos + 1;
    int positionPos = firstRowIdPos + 1;
    long expectedRowId = (Long) actual[firstRowIdPos] + (Long) actual[positionPos];
    assertThat(actual[rowIdPos]).isEqualTo(expectedRowId);
  }
}
