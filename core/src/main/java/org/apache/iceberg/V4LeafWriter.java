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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

/**
 * A {@link ManifestWriter} facade for v4+ leaf data/delete manifests. Bridges the {@code
 * ManifestWriter<F>} contract (used by legacy code paths, {@link RollingManifestWriter}, and
 * Spark/Flink manifest utilities) to the reshaped {@link TrackedFileWriter} core which writes
 * {@code content_entry} rows in {@link TrackedFile} shape.
 *
 * <p>Each {@code add(F)} / {@code existing(F, ...)} / {@code delete(F, ...)} call is translated
 * into a {@link Tracking} struct and wrapped into a {@link TrackedFile} via {@link
 * TrackedFileAdapters}, then handed to the core. DV-on-write is deferred to Phase 6; this facade
 * does not surface {@code addWithDV} / {@code replacedEntry} / {@code modifiedEntry}.
 *
 * @param <F> {@link DataFile} for data manifests or {@link DeleteFile} for delete manifests
 */
class V4LeafWriter<F extends ContentFile<F>> extends ManifestWriter<F> {

  @SuppressWarnings("rawtypes")
  private static final FileAppender NOOP_APPENDER =
      new FileAppender<Object>() {
        @Override
        public void add(Object datum) {
          throw new UnsupportedOperationException(
              "V4LeafWriter routes writes through TrackedFileWriter");
        }

        @Override
        public Metrics metrics() {
          throw new UnsupportedOperationException(
              "V4LeafWriter exposes metrics through TrackedFileWriter");
        }

        @Override
        public long length() {
          throw new UnsupportedOperationException(
              "V4LeafWriter exposes length through TrackedFileWriter");
        }

        @Override
        public void close() {}
      };

  private final LeafManifestWriter core;
  private final TrackedFileAdapters.DataTrackedFile dataAdapter;
  private final TrackedFileAdapters.EqualityDeleteTrackedFile deleteAdapter;

  private V4LeafWriter(
      PartitionSpec spec,
      EncryptedOutputFile file,
      Long snapshotId,
      Long firstRowId,
      Map<String, String> writerProperties,
      LeafManifestWriter core,
      TrackedFileAdapters.DataTrackedFile dataAdapter,
      TrackedFileAdapters.EqualityDeleteTrackedFile deleteAdapter) {
    super(spec, file, snapshotId, firstRowId, writerProperties);
    this.core = core;
    this.dataAdapter = dataAdapter;
    this.deleteAdapter = deleteAdapter;
  }

  /** Opens a v4+ leaf data manifest writer with the table's union partition type. */
  static V4LeafWriter<DataFile> forData(
      PartitionSpec spec,
      Types.StructType unionPartitionType,
      EncryptedOutputFile file,
      Long snapshotId,
      Long firstRowId,
      Map<String, String> writerProperties) {
    Types.StructType partitionType = unionPartitionType;
    MetricsConfig metricsConfig = MetricsConfig.from(writerProperties, spec.schema(), null);
    TrackedFileAdapters.DataTrackedFile adapter =
        TrackedFileAdapters.forDataFile(
            TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
            spec.schema(),
            metricsConfig,
            partitionType);
    LeafManifestWriter core =
        LeafManifestWriter.forData(
            spec, unionPartitionType, file, snapshotId, firstRowId, writerProperties);
    return new V4LeafWriter<>(
        spec, file, snapshotId, firstRowId, writerProperties, core, adapter, null);
  }

  /** Opens a v4+ leaf delete manifest writer with the table's union partition type. */
  static V4LeafWriter<DeleteFile> forDelete(
      PartitionSpec spec,
      Types.StructType unionPartitionType,
      EncryptedOutputFile file,
      Long snapshotId,
      Map<String, String> writerProperties) {
    Types.StructType partitionType = unionPartitionType;
    MetricsConfig metricsConfig = MetricsConfig.from(writerProperties, spec.schema(), null);
    TrackedFileAdapters.EqualityDeleteTrackedFile adapter =
        TrackedFileAdapters.forEqualityDeleteFile(
            TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
            spec.schema(),
            metricsConfig,
            partitionType);
    LeafManifestWriter core =
        LeafManifestWriter.forDelete(spec, unionPartitionType, file, snapshotId, writerProperties);
    return new V4LeafWriter<>(spec, file, snapshotId, null, writerProperties, core, null, adapter);
  }

  // ---- Add / existing / delete overrides ------------------------------------

  @Override
  public void add(F addedFile) {
    Long snapshotId = writerSnapshotId();
    // ADDED rows carry null dataSeq/fileSeq (assigned at commit). Preserve first-row-id from the
    // source file where present (v4+ per-entry firstRowId assignment).
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.ADDED, snapshotId, null, null, null, addedFile.firstRowId(), null, null);
    core.add(wrapContentFile(addedFile, tracking));
  }

  @Override
  public void add(F addedFile, long dataSequenceNumber) {
    Long snapshotId = writerSnapshotId();
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            snapshotId,
            dataSequenceNumber,
            null,
            null,
            addedFile.firstRowId(),
            null,
            null);
    core.add(wrapContentFile(addedFile, tracking));
  }

  @Override
  void add(ManifestEntry<F> entry) {
    // Re-wrap the entry as a fresh ADDED entry (matching parent ManifestWriter semantics),
    // preserving
    // any explicit data sequence number the caller pre-assigned.
    if (entry.dataSequenceNumber() != null && entry.dataSequenceNumber() >= 0) {
      add(entry.file(), entry.dataSequenceNumber());
    } else {
      add(entry.file());
    }
  }

  /**
   * Processes an already-built entry verbatim (preserving its status and sequence numbers), as
   * opposed to {@link #add(ManifestEntry)} which re-wraps it as a fresh ADDED entry. Used by {@link
   * ManifestReader} during rewriteManifests to preserve entries' historical status.
   */
  @Override
  void addEntry(ManifestEntry<F> entry) {
    // ManifestEntry.Status (3 values: ADDED/EXISTING/DELETED) shares IDs with EntryStatus
    // (5 values incl. REPLACED/MODIFIED). Convert by id so a v3-era entry maps to the matching
    // EntryStatus.
    EntryStatus status = EntryStatus.fromId(entry.status().id());
    Tracking tracking =
        new TrackingStruct(
            status,
            entry.snapshotId(),
            entry.dataSequenceNumber(),
            entry.fileSequenceNumber(),
            null,
            entry.file().firstRowId(),
            null,
            null);
    core.add(wrapContentFile(entry.file(), tracking));
  }

  @Override
  public void existing(
      F existingFile, long fileSnapshotId, long dataSequenceNumber, Long fileSequenceNumber) {
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.EXISTING,
            fileSnapshotId,
            dataSequenceNumber,
            fileSequenceNumber,
            null,
            existingFile.firstRowId(),
            null,
            null);
    core.add(wrapContentFile(existingFile, tracking));
  }

  @Override
  void existing(ManifestEntry<F> entry) {
    existing(
        entry.file(), entry.snapshotId(), entry.dataSequenceNumber(), entry.fileSequenceNumber());
  }

  @Override
  public void delete(F deletedFile, long dataSequenceNumber, Long fileSequenceNumber) {
    Long snapshotId = writerSnapshotId();
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.DELETED,
            snapshotId,
            dataSequenceNumber,
            fileSequenceNumber,
            null,
            deletedFile.firstRowId(),
            null,
            null);
    core.add(wrapContentFile(deletedFile, tracking));
  }

  @Override
  void delete(ManifestEntry<F> entry) {
    delete(entry.file(), entry.dataSequenceNumber(), entry.fileSequenceNumber());
  }

  // ---- v4 DV-on-write ------------------------------------------------------

  /**
   * Adds a data file born with a colocated DV in the same commit as a single ADDED entry. Only
   * valid on data manifests; callers reach this via a {@code V4LeafWriter<?>} cast.
   */
  @Override
  void addWithDV(DataFile addedFile, DeletionVector dv) {
    org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkState(
        dataAdapter != null, "addWithDV is only supported for data leaf manifests");
    Long snapshotId = writerSnapshotId();
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.ADDED,
            snapshotId,
            null,
            null,
            snapshotId,
            addedFile.firstRowId(),
            null,
            null);
    core.add(dataAdapter.wrap(addedFile, tracking, dv));
  }

  /**
   * Writes the prior state of a data file in a v4+ REPLACED/MODIFIED pair. Preserves the prior DV
   * when present so downstream change-detection can identify the DV that was superseded by the
   * paired MODIFIED row.
   */
  @Override
  @SuppressWarnings("unchecked")
  void replacedEntry(ManifestEntry<F> entry, DeletionVector priorDv) {
    org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkState(
        dataAdapter != null, "replacedEntry is only supported for data leaf manifests");
    Long snapshotId = writerSnapshotId() != null ? writerSnapshotId() : 0L;
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.REPLACED,
            snapshotId,
            entry.dataSequenceNumber(),
            entry.fileSequenceNumber(),
            null,
            entry.file().firstRowId(),
            null,
            null);
    core.add(dataAdapter.wrap((DataFile) entry.file(), tracking, priorDv));
  }

  /** Writes the new live state of a data file in a v4+ REPLACED/MODIFIED pair with the new DV. */
  @Override
  @SuppressWarnings("unchecked")
  void modifiedEntry(ManifestEntry<F> entry, DeletionVector dv) {
    org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkState(
        dataAdapter != null, "modifiedEntry is only supported for data leaf manifests");
    Long dvSnapshotId = writerSnapshotId() != null ? writerSnapshotId() : 0L;
    Tracking tracking =
        new TrackingStruct(
            EntryStatus.MODIFIED,
            entry.snapshotId(),
            entry.dataSequenceNumber(),
            entry.fileSequenceNumber(),
            dvSnapshotId,
            entry.file().firstRowId(),
            null,
            null);
    core.add(dataAdapter.wrap((DataFile) entry.file(), tracking, dv));
  }

  @SuppressWarnings("unchecked")
  private TrackedFile wrapContentFile(F file, Tracking tracking) {
    if (dataAdapter != null) {
      return dataAdapter.wrap((DataFile) file, tracking);
    }
    return deleteAdapter.wrap((DeleteFile) file, tracking);
  }

  // ---- ManifestWriter contract delegation -----------------------------------

  @Override
  public ManifestFile toManifestFile() {
    return core.toManifestFile();
  }

  @Override
  public Metrics metrics() {
    return core.metrics();
  }

  @Override
  public long length() {
    return core.length();
  }

  @Override
  public void close() throws IOException {
    core.close();
  }

  // The core owns the real content_entry appender; ManifestWriter's constructor still requires an
  // appender, so hand it an inert one that is never exercised (every write path is overridden).
  @Override
  @SuppressWarnings("unchecked")
  protected FileAppender<ManifestEntry<F>> newAppender(PartitionSpec spec, OutputFile file) {
    return (FileAppender<ManifestEntry<F>>) NOOP_APPENDER;
  }

  @Override
  protected ManifestEntry<F> prepare(ManifestEntry<F> entry) {
    throw new UnsupportedOperationException(
        "V4LeafWriter writes through TrackedFileWriter; prepare() is never called");
  }
}
