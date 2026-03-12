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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;

interface ManifestEntry<F extends ContentFile<F>> {
  enum Status {
    EXISTING(0),
    ADDED(1),
    DELETED(2);

    private final int id;

    Status(int id) {
      this.id = id;
    }

    public int id() {
      return id;
    }
  }

  // ids for data-file columns are assigned from 1000
  Types.NestedField STATUS = required(0, "status", Types.IntegerType.get());
  Types.NestedField SNAPSHOT_ID = optional(1, "snapshot_id", Types.LongType.get());
  Types.NestedField SEQUENCE_NUMBER = optional(3, "sequence_number", Types.LongType.get());
  Types.NestedField FILE_SEQUENCE_NUMBER =
      optional(4, "file_sequence_number", Types.LongType.get());
  Types.NestedField COMMIT_TIMESTAMP_MS = optional(5, "commit_timestamp_ms", Types.LongType.get());
  int DATA_FILE_ID = 2;

  // next ID to assign: 6

  static Schema getSchema(StructType partitionType) {
    return wrapFileSchema(DataFile.getType(partitionType));
  }

  static Schema wrapFileSchema(StructType fileType) {
    return new Schema(
        STATUS,
        SNAPSHOT_ID,
        SEQUENCE_NUMBER,
        FILE_SEQUENCE_NUMBER,
        required(DATA_FILE_ID, "data_file", fileType),
        COMMIT_TIMESTAMP_MS);
  }

  /** Returns the status of the file, whether EXISTING, ADDED, or DELETED. */
  Status status();

  /** Returns whether this entry is live */
  default boolean isLive() {
    return status() == Status.ADDED || status() == Status.EXISTING;
  }

  /** Returns id of the snapshot in which the file was added to the table. */
  Long snapshotId();

  /**
   * Set the snapshot id for this manifest entry.
   *
   * @param snapshotId a long snapshot id
   */
  void setSnapshotId(long snapshotId);

  /**
   * Returns the data sequence number of the file.
   *
   * <p>Independently of the entry status, this method represents the sequence number to which the
   * file should apply. Note the data sequence number may differ from the sequence number of the
   * snapshot in which the underlying file was added. New snapshots can add files that belong to
   * older sequence numbers (e.g. compaction). The data sequence number also does not change when
   * the file is marked as deleted.
   *
   * <p>This method can return null if the data sequence number is unknown. This may happen while
   * reading a v2 manifest that did not persist the data sequence number for manifest entries with
   * status DELETED (older Iceberg versions).
   */
  Long dataSequenceNumber();

  /**
   * Sets the data sequence number for this manifest entry.
   *
   * @param dataSequenceNumber a data sequence number
   */
  void setDataSequenceNumber(long dataSequenceNumber);

  /**
   * Returns the file sequence number.
   *
   * <p>The file sequence number represents the sequence number of the snapshot in which the
   * underlying file was added. The file sequence number is always assigned at commit and cannot be
   * provided explicitly, unlike the data sequence number. The file sequence number does not change
   * upon assigning and must be preserved in existing and deleted entries.
   *
   * <p>This method can return null if the file sequence number is unknown. This may happen while
   * reading a v2 manifest that did not persist the file sequence number for manifest entries with
   * status EXISTING or DELETED (older Iceberg versions).
   */
  Long fileSequenceNumber();

  /**
   * Sets the file sequence number for this manifest entry.
   *
   * @param fileSequenceNumber a file sequence number
   */
  void setFileSequenceNumber(long fileSequenceNumber);

  /**
   * Returns the commit timestamp in milliseconds.
   *
   * <p>The commit timestamp represents the snapshot timestamp of the commit that added the file. It
   * is used for inheriting {@code _last_updated_timestamp_ms} in V4 tables.
   *
   * <p>This method can return null if the commit timestamp is unknown.
   */
  default Long commitTimestampMs() {
    return null;
  }

  /**
   * Sets the commit timestamp in milliseconds for this manifest entry.
   *
   * @param commitTimestampMs a commit timestamp in milliseconds
   */
  default void setCommitTimestampMs(long commitTimestampMs) {}

  /** Returns a file. */
  F file();

  ManifestEntry<F> copy();

  ManifestEntry<F> copyWithoutStats();
}
