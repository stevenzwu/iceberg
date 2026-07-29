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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads v4+ root manifest files, yielding one {@link ManifestFile} per {@code DATA_MANIFEST} or
 * {@code DELETE_MANIFEST} content_entry row.
 *
 * <p>Direct DATA content rows (the small-write inline optimization) are aggregated into a single
 * synthetic {@link RootDirectRowsManifestFile} entry appended after the leaf references so
 * consumers of {@link Snapshot#dataManifests} — notably scan planning via {@link ManifestGroup} —
 * see the inline data files. Direct {@code EQUALITY_DELETES} rows are skipped at DEBUG level;
 * delete-side surfacing is deferred to Phase 6.
 *
 * <p>Leaf-manifest-entry rows leave the partition column null on write. For the direct-row
 * aggregation the partition tuple is not decoded here — only the aggregate row/file counts and
 * {@code spec_id} are collected, which is sufficient to synthesize the virtual manifest's
 * file-level metadata. Actual per-row partition decoding for the virtual manifest happens when
 * {@link ManifestFiles#read} opens a {@link V4ManifestReader} with the caller-supplied {@code
 * specsById}.
 */
class RootManifestReader {
  private static final Logger LOG = LoggerFactory.getLogger(RootManifestReader.class);

  private RootManifestReader() {}

  /**
   * Reads a v4+ root manifest and returns the list of {@link ManifestFile} objects. When the root
   * carries direct DATA rows (small-write optimization), a synthetic {@link
   * RootDirectRowsManifestFile} covering those rows is appended so consumers of {@link
   * Snapshot#dataManifests} pick them up alongside real leaf references.
   *
   * @param rootManifest the root manifest input file
   * @return list of manifest files (data and delete), in the order they appear in the root manifest
   */
  static List<ManifestFile> read(InputFile rootManifest) {
    Preconditions.checkArgument(rootManifest != null, "Invalid root manifest input file: null");
    // Spec-agnostic read: this reader has no specsById, so it cannot build the real union partition
    // type. It never decodes partition tuples (only leaf refs + direct-row counts), so it projects
    // the partition column through EMPTY_PARTITION_PLACEHOLDER — a struct that field-id-projects
    // any
    // physical partition column (partitioned direct rows) to null, and reads null when absent
    // (unpartitioned). Per-row partition decoding for scan planning happens later via a
    // specsById-aware V4ManifestReader.
    Types.StructType readPartitionType = TrackedFileWriter.EMPTY_PARTITION_PLACEHOLDER;
    Schema contentEntrySchema =
        TrackedFile.schema(readPartitionType, TrackedFileWriter.ROOT_CONTENT_STATS_TYPE);

    CloseableIterable<TrackedFileStruct> rows =
        InternalData.read(FileFormat.PARQUET, rootManifest)
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .build();

    List<ManifestFile> manifests = Lists.newArrayList();
    DirectRowAggregator directAgg = new DirectRowAggregator();
    try {
      for (TrackedFileStruct row : rows) {
        FileContent content = row.contentType();
        if (content == FileContent.DATA_MANIFEST || content == FileContent.DELETE_MANIFEST) {
          manifests.add(toManifestFile(row));
        } else if (content == FileContent.DATA) {
          directAgg.accumulate(row);
        } else {
          // Direct EQUALITY_DELETES rows are deferred to Phase 6. Skip them at DEBUG level.
          LOG.debug(
              "Skipping direct data-file entry with content_type={} in root manifest {}",
              content,
              rootManifest.location());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read root manifest: " + rootManifest.location(), e);
    } finally {
      try {
        rows.close();
      } catch (IOException e) {
        LOG.warn("Failed to close root manifest reader for {}", rootManifest.location(), e);
      }
    }

    ManifestFile virtual = directAgg.build(rootManifest);
    if (virtual != null) {
      manifests.add(virtual);
    }

    return manifests;
  }

  private static ManifestFile toManifestFile(TrackedFileStruct row) {
    Tracking tracking = row.tracking();
    Preconditions.checkArgument(
        tracking != null, "Invalid root manifest entry: missing tracking struct");

    ManifestContent manifestContent =
        row.contentType() == FileContent.DATA_MANIFEST
            ? ManifestContent.DATA
            : ManifestContent.DELETES;

    Long snapshotId = tracking.snapshotId();
    Long sequenceNumber = tracking.dataSequenceNumber();
    long seqNum = sequenceNumber != null ? sequenceNumber : 0L;

    ManifestInfo info = row.manifestInfo();
    int addedFiles = info != null ? info.addedFilesCount() : 0;
    int existingFiles = info != null ? info.existingFilesCount() : 0;
    int deletedFiles = info != null ? info.deletedFilesCount() : 0;
    long addedRows = info != null ? info.addedRowsCount() : 0L;
    long existingRows = info != null ? info.existingRowsCount() : 0L;
    long deletedRows = info != null ? info.deletedRowsCount() : 0L;
    long minSequenceNumber = info != null ? info.minSequenceNumber() : seqNum;
    Integer replacedFiles = projectedReplacedFilesCount(info);
    Long replacedRows = projectedReplacedRowsCount(info);

    Integer specId = row.specId();
    int partitionSpecId = specId != null ? specId : 0;

    return new GenericManifestFile(
        row.location(),
        row.fileSizeInBytes(),
        partitionSpecId,
        manifestContent,
        seqNum,
        minSequenceNumber,
        snapshotId,
        null /* no partition summaries in root manifest entries */,
        row.keyMetadata(),
        addedFiles,
        addedRows,
        existingFiles,
        existingRows,
        deletedFiles,
        deletedRows,
        tracking.firstRowId(),
        row.recordCount(),
        row.formatVersion(),
        replacedFiles,
        replacedRows);
  }

  /**
   * Reads direct DATA content rows from a v4+ root manifest (the small-write inline data files) and
   * yields them as raw {@link TrackedFile}s so callers can preserve the original tracking (status,
   * snapshot-id, sequence-numbers, first-row-id) needed for carry-over into a child snapshot.
   * Consumers that need {@link DataFile} views for filter evaluation or scan planning use {@link
   * #readDirectDataRows} instead — it wraps each TrackedFile through {@link
   * TrackedFileAdapters#asDataFile}.
   *
   * <p>The reader projects the actual union partition type from {@code specsById} so per-row
   * partition tuples decode correctly, but uses the placeholder {@link
   * TrackedFileWriter#ROOT_CONTENT_STATS_TYPE} for content stats — column-stats maps come back as
   * null. Sufficient for partition-only filters, scan-planning identity, and carry-over.
   */
  static List<TrackedFile> readDirectRows(
      InputFile rootManifest, Map<Integer, PartitionSpec> specsById) {
    Preconditions.checkArgument(rootManifest != null, "Invalid root manifest input file: null");
    Preconditions.checkArgument(
        specsById != null && !specsById.isEmpty(), "Invalid specs: null or empty");

    Types.StructType readPartitionType =
        TrackedFileWriter.emptyPartitionPlaceholderIfNeeded(
            Partitioning.unionPartitionTypes(specsById.values()));
    Schema contentEntrySchema =
        TrackedFile.schema(readPartitionType, TrackedFileWriter.ROOT_CONTENT_STATS_TYPE);

    CloseableIterable<TrackedFileStruct> rows =
        InternalData.read(FileFormat.PARQUET, rootManifest)
            .project(contentEntrySchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .build();

    List<TrackedFile> directRows = Lists.newArrayList();
    try {
      for (TrackedFileStruct row : rows) {
        if (row.contentType() == FileContent.DATA) {
          directRows.add((TrackedFile) row.copy());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to read direct data rows from root manifest: " + rootManifest.location(), e);
    } finally {
      try {
        rows.close();
      } catch (IOException e) {
        LOG.warn("Failed to close root manifest reader for {}", rootManifest.location(), e);
      }
    }

    return directRows;
  }

  /**
   * Convenience wrapper around {@link #readDirectRows} that yields {@link DataFile} views for
   * partition-only filters and scan-planning identity.
   */
  static List<DataFile> readDirectDataRows(
      InputFile rootManifest, Map<Integer, PartitionSpec> specsById) {
    List<TrackedFile> rows = readDirectRows(rootManifest, specsById);
    List<DataFile> dataFiles = Lists.newArrayListWithCapacity(rows.size());
    for (TrackedFile row : rows) {
      dataFiles.add(TrackedFileAdapters.asDataFile(row, specsById));
    }
    return dataFiles;
  }

  /** Returns the REPLACED file count from {@code info} when present and non-zero, else null. */
  private static Integer projectedReplacedFilesCount(ManifestInfo info) {
    if (info == null) {
      return null;
    }

    return info.replacedFilesCount() > 0 ? info.replacedFilesCount() : null;
  }

  /**
   * Returns the REPLACED row count from {@code info} when its file count is non-zero, else null.
   */
  private static Long projectedReplacedRowsCount(ManifestInfo info) {
    if (info == null) {
      return null;
    }

    return info.replacedFilesCount() > 0 ? info.replacedRowsCount() : null;
  }

  /**
   * Aggregates per-row status counts and sequence numbers over direct DATA rows in a promoted root.
   * Used to synthesize a {@link RootDirectRowsManifestFile} that exposes those inline data files
   * via {@link Snapshot#dataManifests}.
   */
  private static class DirectRowAggregator {
    private int addedFiles = 0;
    private int existingFiles = 0;
    private int deletedFiles = 0;
    private int replacedFiles = 0;
    private long addedRows = 0L;
    private long existingRows = 0L;
    private long deletedRows = 0L;
    private long replacedRows = 0L;
    private Integer specId = null;
    // snapshotId prefers whichever row was authored by the current commit — ADDED, REPLACED, or
    // DELETED rows all carry the current commit's snapshot id at write time. MODIFIED and EXISTING
    // rows preserve the source's original snapshot id and must not be picked, since the virtual
    // manifest's snapshotId is used to filter "manifests written by this snapshot" in delta APIs
    // (BaseSnapshot / SnapshotChanges). Falls back to any observed tracking snapshot when no
    // current-commit row is present.
    private Long currentCommitSnapshotId = null;
    private Long fallbackSnapshotId = null;
    private long sequenceNumber = 0L;
    private long minSequenceNumber = Long.MAX_VALUE;
    private Long firstRowId = null;
    private int formatVersion = TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE;
    private boolean sawRow = false;

    void accumulate(TrackedFileStruct row) {
      sawRow = true;
      Tracking tracking = row.tracking();
      accumulateStatusCounts(tracking, row.recordCount());
      accumulateTracking(tracking);
      if (specId == null && row.specId() != null) {
        specId = row.specId();
      }

      // Preserve the row's format version so callers can dispatch correctly. Direct rows are v4+.
      int rowFormatVersion = row.formatVersion();
      if (rowFormatVersion > formatVersion) {
        formatVersion = rowFormatVersion;
      }
    }

    private void accumulateStatusCounts(Tracking tracking, long rowCount) {
      // Defensive: treat missing status as ADDED so file count still bumps.
      EntryStatus status = tracking != null ? tracking.status() : EntryStatus.ADDED;
      if (status == null) {
        status = EntryStatus.ADDED;
      }

      switch (status) {
        case ADDED:
          addedFiles += 1;
          addedRows += rowCount;
          if (currentCommitSnapshotId == null && tracking != null) {
            currentCommitSnapshotId = tracking.snapshotId();
          }
          break;
        case EXISTING:
        case MODIFIED:
          existingFiles += 1;
          existingRows += rowCount;
          break;
        case DELETED:
          deletedFiles += 1;
          deletedRows += rowCount;
          // DELETED direct rows (from retirement) carry the current commit's snapshot id.
          if (currentCommitSnapshotId == null && tracking != null) {
            currentCommitSnapshotId = tracking.snapshotId();
          }
          break;
        case REPLACED:
          replacedFiles += 1;
          replacedRows += rowCount;
          // REPLACED direct rows (from DV rewrite) carry the current commit's snapshot id and are
          // tracked separately so the virtual manifest's replacedFilesCount surfaces to concurrent
          // DV validation (validateNoConflictingDeleteFiles filters by replacedFilesCount > 0).
          if (currentCommitSnapshotId == null && tracking != null) {
            currentCommitSnapshotId = tracking.snapshotId();
          }
          break;
        default:
          // Unknown statuses are counted under EXISTING so they still contribute to file counts.
          existingFiles += 1;
          existingRows += rowCount;
      }
    }

    private void accumulateTracking(Tracking tracking) {
      if (tracking == null) {
        return;
      }

      if (fallbackSnapshotId == null) {
        fallbackSnapshotId = tracking.snapshotId();
      }

      Long dataSeq = tracking.dataSequenceNumber();
      if (dataSeq != null) {
        sequenceNumber = Math.max(sequenceNumber, dataSeq);
        minSequenceNumber = Math.min(minSequenceNumber, dataSeq);
      }

      Long rowFirstId = tracking.firstRowId();
      if (rowFirstId != null && (firstRowId == null || rowFirstId < firstRowId)) {
        firstRowId = rowFirstId;
      }
    }

    ManifestFile build(InputFile rootManifest) {
      if (!sawRow) {
        return null;
      }
      // Skip virtual-manifest synthesis when the root carries only retirement rows
      // (DELETED / REPLACED) — matches v3 semantics where a manifest whose entries are all
      // not-live is filtered by MergingSnapshotProducer.shouldKeep. Callers of dataManifests()
      // expect only manifests with live content; a virtual over pure-retirement rows would
      // wrongly surface deleted files to metadata tables and scan planning.
      if (addedFiles == 0 && existingFiles == 0) {
        return null;
      }

      long resolvedMinSequenceNumber =
          minSequenceNumber == Long.MAX_VALUE ? sequenceNumber : minSequenceNumber;
      int resolvedSpecId = specId != null ? specId : 0;
      Long resolvedSnapshotId =
          currentCommitSnapshotId != null ? currentCommitSnapshotId : fallbackSnapshotId;
      long rootLength;
      try {
        rootLength = rootManifest.getLength();
      } catch (RuntimeException e) {
        // Not all InputFile implementations support getLength() before a read; fall back to 0.
        rootLength = 0L;
      }

      return new RootDirectRowsManifestFile(
          rootManifest.location(),
          rootLength,
          resolvedSpecId,
          sequenceNumber,
          resolvedMinSequenceNumber,
          resolvedSnapshotId,
          addedFiles,
          addedRows,
          existingFiles,
          existingRows,
          deletedFiles,
          deletedRows,
          firstRowId,
          formatVersion,
          replacedFiles > 0 ? replacedFiles : null,
          replacedRows > 0 ? replacedRows : null);
    }
  }
}
