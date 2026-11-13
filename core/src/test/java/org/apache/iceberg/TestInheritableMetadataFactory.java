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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

class TestInheritableMetadataFactory {
  private static final long SNAPSHOT_ID = 42L;
  private static final long SEQUENCE_NUMBER = 10L;
  private static final long SNAPSHOT_TIMESTAMP_MICROS = 1_700_000_000_000_000L;
  private static final String MANIFEST_PATH = "s3://bucket/table/manifest.avro";

  @Test
  void inheritSnapshotTimestamp() {
    ManifestEntry<DataFile> entry = addedEntry();

    InheritableMetadataFactory.fromManifest(manifest(SNAPSHOT_TIMESTAMP_MICROS)).apply(entry);

    assertThat(entry.snapshotTimestampMicros()).isEqualTo(SNAPSHOT_TIMESTAMP_MICROS);
    assertThat(entry.file().snapshotTimestampMicros()).isEqualTo(SNAPSHOT_TIMESTAMP_MICROS);
  }

  @Test
  void clearSnapshotTimestampWhenManifestHasNone() {
    ManifestEntry<DataFile> entry = addedEntry();
    entry.setSnapshotTimestampMicros(SNAPSHOT_TIMESTAMP_MICROS);

    InheritableMetadataFactory.fromManifest(manifest(null)).apply(entry);

    assertThat(entry.snapshotTimestampMicros()).isNull();
    assertThat(entry.file().snapshotTimestampMicros()).isNull();
  }

  private static ManifestFile manifest(Long snapshotTimestampMicros) {
    ManifestFile manifest = mock(ManifestFile.class);
    when(manifest.partitionSpecId()).thenReturn(0);
    when(manifest.snapshotId()).thenReturn(SNAPSHOT_ID);
    when(manifest.sequenceNumber()).thenReturn(SEQUENCE_NUMBER);
    when(manifest.path()).thenReturn(MANIFEST_PATH);
    when(manifest.snapshotTimestampMicros()).thenReturn(snapshotTimestampMicros);
    return manifest;
  }

  private static ManifestEntry<DataFile> addedEntry() {
    DataFile file =
        new GenericDataFile(
            PartitionSpec.unpartitioned().specId(),
            "s3://bucket/data/file.parquet",
            FileFormat.PARQUET,
            PartitionData.EMPTY,
            1024L,
            new Metrics(100L),
            null,
            null,
            null,
            null);
    return new GenericManifestEntry<DataFile>(
            ManifestEntry.getSchema(PartitionSpec.unpartitioned().partitionType()).asStruct())
        .wrapAppend(null, file);
  }
}
