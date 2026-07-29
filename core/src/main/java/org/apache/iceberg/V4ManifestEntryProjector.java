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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructProjection;

/**
 * Decodes a v4+ manifest file's {@code content_entry} rows and projects them into legacy {@link
 * ManifestEntry} views, colocated deletion-vector views, and per-snapshot DV change pairs.
 *
 * <p>Owns the parquet decode: closing this projector closes the underlying row iterable. Applies
 * {@link InheritableMetadata} to fill in sequence numbers and status metadata on the emitted
 * entries.
 */
class V4ManifestEntryProjector extends CloseableGroup {
  static final int SUPPORTED_FORMAT_VERSION = 4;

  private final InputFile file;
  private final ManifestContent contentType;
  private final int defaultSpecId;
  private final Map<Integer, PartitionSpec> specsById;
  private final InheritableMetadata inheritableMetadata;
  // When non-null, rawRows() returns these pre-decoded rows instead of opening the file. Used to
  // short-circuit the second parquet decode of a v4 root manifest when the direct rows were
  // already materialized during Snapshot.dataManifests resolution.
  private final List<TrackedFile> cachedRows;

  V4ManifestEntryProjector(
      InputFile file,
      ManifestContent contentType,
      int defaultSpecId,
      Map<Integer, PartitionSpec> specsById,
      InheritableMetadata inheritableMetadata) {
    this(file, contentType, defaultSpecId, specsById, inheritableMetadata, null);
  }

  /**
   * Builds a projector over pre-decoded rows (from {@link RootManifestReader#read}). Skips the
   * second parquet open that would otherwise re-decode the same file.
   */
  V4ManifestEntryProjector(
      InputFile file,
      ManifestContent contentType,
      int defaultSpecId,
      Map<Integer, PartitionSpec> specsById,
      InheritableMetadata inheritableMetadata,
      List<TrackedFile> cachedRows) {
    Preconditions.checkArgument(file != null, "Invalid input file: null");
    Preconditions.checkArgument(contentType != null, "Invalid content type: null");
    Preconditions.checkArgument(inheritableMetadata != null, "Invalid inheritable metadata: null");
    this.file = file;
    this.contentType = contentType;
    this.defaultSpecId = defaultSpecId;
    this.specsById = specsById;
    this.inheritableMetadata = inheritableMetadata;
    this.cachedRows = cachedRows;
  }

  /** Returns all entries (including deleted) as data manifest entries. */
  CloseableIterable<ManifestEntry<DataFile>> dataEntries() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read data entries from a delete manifest: %s",
        file.location());
    return readEntries();
  }

  /**
   * Returns direct DATA rows from a v4+ promoted root manifest as data manifest entries. Filters
   * co-resident {@code DATA_MANIFEST} and {@code DELETE_MANIFEST} rows out before conversion, so
   * the caller sees only inline data-file entries. Preserves v4 tracking status directly (ADDED /
   * EXISTING / DELETED) — no collapsing.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  CloseableIterable<ManifestEntry<DataFile>> directDataEntriesFromRoot() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read direct data entries from a delete manifest: %s",
        file.location());
    CloseableIterable<TrackedFile> dataRows =
        CloseableIterable.filter(rawRows(), row -> row.contentType() == FileContent.DATA);
    return (CloseableIterable<ManifestEntry<DataFile>>)
        (CloseableIterable) CloseableIterable.transform(dataRows, this::toManifestEntry);
  }

  /** Returns all entries (including deleted) as delete manifest entries. */
  CloseableIterable<ManifestEntry<DeleteFile>> deleteEntries() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DELETES,
        "Cannot read delete entries from a data manifest: %s",
        file.location());
    @SuppressWarnings({"unchecked", "rawtypes"})
    CloseableIterable<ManifestEntry<DeleteFile>> result =
        (CloseableIterable<ManifestEntry<DeleteFile>>) (CloseableIterable) readEntries();
    return result;
  }

  /**
   * Returns the colocated deletion vectors carried by live data rows in this data manifest, each
   * projected as a {@link DeleteFile} with content {@link FileContent#POSITION_DELETES} and format
   * {@link FileFormat#PUFFIN}. REPLACED rows are excluded — only live (ADDED, EXISTING, MODIFIED)
   * rows surface their attached DV. Rows without a {@code deletion_vector} are skipped.
   */
  CloseableIterable<DeleteFile> colocatedDVDeleteFiles() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read deletion vectors from a delete manifest: %s",
        file.location());
    CloseableIterable<DeleteFile> dvs =
        CloseableIterable.transform(
            rawRows(),
            row -> {
              if (!isLiveDataRowWithDV(row)) {
                return null;
              }

              return toDVDeleteFile(row);
            });

    return CloseableIterable.filter(dvs, dv -> dv != null);
  }

  /**
   * Returns colocated DV changes encoded as {@code (status, DeleteFile)} pairs, suitable for
   * computing per-snapshot delete-file deltas: ADDED DVs (ADDED and MODIFIED rows) and DELETED DVs
   * (REPLACED rows). EXISTING rows and rows without a DV are skipped.
   */
  CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> colocatedDVChanges() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read deletion vector changes from a delete manifest: %s",
        file.location());
    CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> changes =
        CloseableIterable.transform(
            rawRows(),
            row -> {
              ManifestEntry.Status changeStatus = dvChangeStatus(row);
              if (changeStatus == null) {
                return null;
              }

              return Pair.of(changeStatus, toDVDeleteFile(row));
            });

    return CloseableIterable.filter(changes, p -> p != null);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private <F extends ContentFile<F>> CloseableIterable<ManifestEntry<F>> readEntries() {
    // toManifestEntry fully materializes each row into a new ManifestEntry (fresh file, metrics,
    // and partition), so the row can be passed directly without a defensive copy.
    return (CloseableIterable<ManifestEntry<F>>)
        (CloseableIterable) CloseableIterable.transform(rawRows(), this::toManifestEntry);
  }

  /**
   * Returns the raw {@code content_entry} rows as defensive copies, preserving each row's exact
   * status, colocated deletion vector, partition, and stats. No filtering is applied; equality and
   * DV projection happen in this class's per-view helpers.
   */
  CloseableIterable<TrackedFile> rawRows() {
    return CloseableIterable.transform(buildRows(), TrackedFile::copy);
  }

  /**
   * Builds the raw content_entry row iterable for this manifest and registers it for close. Returns
   * pre-decoded cached rows when available; otherwise opens the parquet file with a schema derived
   * from the default spec.
   */
  private CloseableIterable<TrackedFile> buildRows() {
    if (cachedRows != null) {
      // Rows were already decoded during Snapshot.dataManifests resolution and cached on the
      // RootDirectRowsManifestFile. Return the cache directly; no second parquet decode.
      return CloseableIterable.withNoopClose(cachedRows);
    }

    PartitionSpec defaultSpec = resolveDefaultSpec();
    Types.StructType statsType =
        StatsUtil.statsReadSchema(
            defaultSpec.schema(), TypeUtil.getProjectedIds(defaultSpec.schema()));
    Schema contentEntrySchema = buildContentEntrySchema(defaultSpec, statsType);
    // v4+ leaf manifests are always Parquet; match the content_entry writer's format.
    return openRows(FileFormat.PARQUET, contentEntrySchema, statsType);
  }

  /**
   * Decodes {@code content_entry} rows for the given projection and registers the row iterable for
   * close. When {@code statsType} is non-null the per-column stats sub-structs are registered so
   * the projected {@code content_stats} column round-trips.
   */
  private CloseableIterable<TrackedFile> openRows(
      FileFormat format, Schema projection, Types.StructType statsType) {
    InternalData.ReadBuilder builder =
        InternalData.read(format, file)
            .project(projection)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class);

    if (statsType != null) {
      // Read each per-column stats sub-struct as a FieldStatsStruct so ContentStatsStruct.set can
      // store them directly; unregistered nested structs would default to GenericRecord.
      builder.setCustomType(TrackedFile.CONTENT_STATS_ID, ContentStatsStruct.class);
      for (Types.NestedField statsField : statsType.fields()) {
        builder.setCustomType(statsField.fieldId(), FieldStatsStruct.class);
      }
    }

    CloseableIterable<TrackedFile> rows = builder.build();
    addCloseable(rows);
    return rows;
  }

  private Schema buildContentEntrySchema(PartitionSpec spec, Types.StructType statsType) {
    // v4+ leaf manifests encode partition tuples with the union partition type (a struct covering
    // every live spec's fields). Read with the same union so per-spec subsets land in the correct
    // positions; per-spec projection happens later in toPartitionData. An empty union is projected
    // through the writer's empty-partition placeholder so a physical partition column reads null —
    // this projector can be invoked without the real spec (e.g. the deprecated
    // Snapshot.addedDeleteFiles path).
    Types.StructType unionType =
        specsById != null && !specsById.isEmpty()
            ? Partitioning.unionPartitionTypes(specsById.values())
            : spec.partitionType();
    Types.StructType readPartitionType =
        TrackedFileWriter.emptyPartitionPlaceholderIfNeeded(unionType);
    // When callers pass null specsById (e.g. deprecated Snapshot.addedDataFiles), the derived
    // stats schema is empty; TrackedFile.schema() would surface it as UnknownType which the
    // Parquet reader cannot prune. Fall back to the root manifest's null-stats placeholder so the
    // read schema always projects a struct column.
    Types.StructType readStatsType =
        statsType.fields().isEmpty() ? TrackedFileWriter.ROOT_CONTENT_STATS_TYPE : statsType;
    return TrackedFile.schema(readPartitionType, readStatsType);
  }

  // Builds a GenericDeleteFile from a v4+ colocated DV row. Using GenericDeleteFile (a BaseFile)
  // rather than a lighter adapter lets InheritableMetadata propagate the dataSequenceNumber from
  // the parent manifest to the file — required for DeleteFileIndex's sequence-number checks.
  private DeleteFile toDVDeleteFile(TrackedFile row) {
    Integer specId = row.specId();
    PartitionSpec spec = specById(specId);
    if (spec == null) {
      spec = resolveDefaultSpec();
    }

    DeletionVector dv = row.deletionVector();
    PartitionData partition = toPartitionData(row, spec);

    GenericDeleteFile dvFile =
        new GenericDeleteFile(
            spec.specId(),
            FileContent.POSITION_DELETES,
            dv.location(),
            FileFormat.PUFFIN,
            partition,
            dv.sizeInBytes(),
            new Metrics(dv.cardinality(), null, null, null, null, null, null),
            null /* no equality field ids */,
            null /* DVs are unsorted per spec */,
            null /* no split offsets */,
            null /* no key metadata */,
            row.location() /* referenced data file */,
            dv.offset(),
            dv.sizeInBytes());

    // Treat the DV row as a freshly ADDED entry so InheritableMetadata.fromManifest assigns the
    // manifest's sequenceNumber to the DV. This matches v3 standalone DV-delete-manifest behavior.
    GenericManifestEntry<DeleteFile> entry = new GenericManifestEntry<>(spec.partitionType());
    entry.wrapAppendPreservingFirstRowId(null, null, dvFile);
    inheritableMetadata.apply(entry);
    return entry.file();
  }

  private static boolean isLiveDataRowWithDV(TrackedFile row) {
    if (row.contentType() != FileContent.DATA) {
      return false;
    }

    if (row.deletionVector() == null) {
      return false;
    }

    Tracking tracking = row.tracking();
    if (tracking == null) {
      return false;
    }

    EntryStatus status = tracking.status();
    return status == EntryStatus.ADDED
        || status == EntryStatus.EXISTING
        || status == EntryStatus.MODIFIED;
  }

  // Maps a content_entry row to a per-snapshot DV-change status (ADDED for newly-live DVs,
  // DELETED for superseded DVs), or returns null for rows that do not represent a DV change in
  // this snapshot (no DV, equality-delete rows, or carried-over EXISTING/DELETED rows).
  private static ManifestEntry.Status dvChangeStatus(TrackedFile row) {
    if (row.contentType() != FileContent.DATA) {
      return null;
    }

    if (row.deletionVector() == null) {
      return null;
    }

    Tracking tracking = row.tracking();
    if (tracking == null) {
      return null;
    }

    switch (tracking.status()) {
      case ADDED:
      case MODIFIED:
        // ADDED-with-DV (born-with-DV) and MODIFIED-with-DV (DV added/updated) — both surface the
        // DV as newly live in this snapshot.
        return ManifestEntry.Status.ADDED;
      case REPLACED:
        // REPLACED-with-DV — the prior DV preserved on the REPLACED row. Surfaced as removed.
        return ManifestEntry.Status.DELETED;
      default:
        // EXISTING (DV carried over unchanged) and DELETED are ignored.
        return null;
    }
  }

  private PartitionSpec resolveDefaultSpec() {
    if (specsById != null && !specsById.isEmpty()) {
      PartitionSpec spec = specsById.get(defaultSpecId);
      if (spec != null) {
        return spec;
      }

      return specsById.values().iterator().next();
    }

    return PartitionSpec.unpartitioned();
  }

  private PartitionSpec specById(Integer specId) {
    if (specsById != null && specId != null) {
      return specsById.get(specId);
    }

    return null;
  }

  private ManifestEntry<?> toManifestEntry(TrackedFile row) {
    int formatVersion = row.formatVersion();
    Preconditions.checkArgument(
        formatVersion <= SUPPORTED_FORMAT_VERSION,
        "Unsupported format_version: %s (max supported: %s)",
        formatVersion,
        SUPPORTED_FORMAT_VERSION);

    Tracking tracking = row.tracking();
    Preconditions.checkArgument(
        tracking != null,
        "Invalid content_entry row: missing tracking struct in %s",
        file.location());

    FileContent content = row.contentType();
    Preconditions.checkArgument(
        content != null,
        "Invalid content_entry row: missing content_type in %s",
        file.location());

    Integer specId = row.specId();
    PartitionSpec spec = specById(specId);
    if (spec == null) {
      spec = resolveDefaultSpec();
    }

    Long snapshotId = tracking.snapshotId();
    Long dataSequenceNumber = tracking.dataSequenceNumber();
    Long fileSequenceNumber = tracking.fileSequenceNumber();
    ManifestEntry.Status manifestStatus = toManifestStatus(tracking.status());

    if (content == FileContent.DATA) {
      DataFile dataFile = toDataFile(row, spec, tracking);
      GenericManifestEntry<DataFile> entry = new GenericManifestEntry<>(spec.partitionType());
      setEntry(entry, manifestStatus, snapshotId, dataSequenceNumber, fileSequenceNumber, dataFile);
      if (isLiveDataRowWithDV(row)) {
        // Attach the colocated DV inline so scan planning can build FileScanTask.deletes()
        // directly from the entry, without a path-keyed round trip through DeleteFileIndex.
        entry.setDeletionVector(toDVDeleteFile(row));
      }

      return inheritableMetadata.apply(entry);
    } else if (content == FileContent.EQUALITY_DELETES) {
      DeleteFile deleteFile = toEqualityDeleteFile(row, spec);
      GenericManifestEntry<DeleteFile> entry = new GenericManifestEntry<>(spec.partitionType());
      setEntry(
          entry, manifestStatus, snapshotId, dataSequenceNumber, fileSequenceNumber, deleteFile);
      return inheritableMetadata.apply(entry);
    } else {
      throw new IllegalArgumentException(
          "Unsupported content_type in leaf manifest: " + content + " in " + file.location());
    }
  }

  private static <F extends ContentFile<F>> void setEntry(
      GenericManifestEntry<F> entry,
      ManifestEntry.Status status,
      Long snapshotId,
      Long dataSequenceNumber,
      Long fileSequenceNumber,
      F file) {
    switch (status) {
      case ADDED:
        // Preserve the firstRowId already set on the file (read from the tracking struct) — v4+
        // stores firstRowId per-entry rather than at manifest level.
        entry.wrapAppendPreservingFirstRowId(snapshotId, dataSequenceNumber, file);
        break;
      case EXISTING:
        entry.wrapExisting(snapshotId, dataSequenceNumber, fileSequenceNumber, file);
        break;
      case DELETED:
        entry.wrapDelete(snapshotId, dataSequenceNumber, fileSequenceNumber, file);
        break;
      default:
        throw new IllegalArgumentException("Unknown manifest status: " + status);
    }
  }

  private DataFile toDataFile(TrackedFile row, PartitionSpec spec, Tracking tracking) {
    Metrics metrics = toMetrics(row);
    PartitionData partition = toPartitionData(row, spec);
    Long firstRowId = tracking.firstRowId();

    return new GenericDataFile(
        spec.specId(),
        row.location(),
        row.fileFormat(),
        partition,
        row.fileSizeInBytes(),
        metrics,
        row.keyMetadata(),
        row.splitOffsets(),
        row.sortOrderId(),
        firstRowId);
  }

  private DeleteFile toEqualityDeleteFile(TrackedFile row, PartitionSpec spec) {
    Metrics metrics = toMetrics(row);
    PartitionData partition = toPartitionData(row, spec);
    List<Integer> equalityIdList = row.equalityIds();
    int[] equalityIds = null;
    if (equalityIdList != null) {
      equalityIds = new int[equalityIdList.size()];
      for (int i = 0; i < equalityIdList.size(); i++) {
        equalityIds[i] = equalityIdList.get(i);
      }
    }

    return new GenericDeleteFile(
        spec.specId(),
        FileContent.EQUALITY_DELETES,
        row.location(),
        row.fileFormat(),
        partition,
        row.fileSizeInBytes(),
        metrics,
        equalityIds,
        row.sortOrderId(),
        row.splitOffsets(),
        row.keyMetadata(),
        null /* no referenced data file */,
        null /* no content offset */,
        null /* no content size */);
  }

  private static Metrics toMetrics(TrackedFile row) {
    ContentStats contentStats = row.contentStats();
    return new Metrics(
        row.recordCount(),
        null /* column sizes not stored in content_stats */,
        ContentStatsBackedMap.valueCounts(contentStats),
        ContentStatsBackedMap.nullValueCounts(contentStats),
        ContentStatsBackedMap.nanValueCounts(contentStats),
        ContentStatsBackedMap.lowerBounds(contentStats),
        ContentStatsBackedMap.upperBounds(contentStats));
  }

  private static PartitionData toPartitionData(TrackedFile row, PartitionSpec spec) {
    StructLike rowPartition = row.partition();
    Types.StructType specType = spec.partitionType();
    if (rowPartition instanceof PartitionData) {
      PartitionData unionPartition = (PartitionData) rowPartition;
      // The on-disk partition is encoded with the union partition type. Project back to the
      // writer spec's partition type so downstream consumers see a partition struct that matches
      // the file's own spec (and not a wider union shape).
      if (unionPartition.getPartitionType().equals(specType)) {
        return unionPartition.copy();
      }

      PartitionData result = new PartitionData(specType);
      StructProjection projection =
          StructProjection.createAllowMissing(unionPartition.getPartitionType(), specType);
      projection.wrap(unionPartition);
      for (int pos = 0; pos < specType.fields().size(); pos += 1) {
        result.set(pos, projection.get(pos, Object.class));
      }

      return result;
    }

    return new PartitionData(specType);
  }

  private static ManifestEntry.Status toManifestStatus(EntryStatus entryStatus) {
    switch (entryStatus) {
      case ADDED:
        return ManifestEntry.Status.ADDED;
      case EXISTING:
        return ManifestEntry.Status.EXISTING;
      case DELETED:
        return ManifestEntry.Status.DELETED;
      case REPLACED:
        // REPLACED is the prior state of a modified entry — non-live (isLive() == false). Surface
        // as DELETED so isLive() correctly returns false for legacy consumers.
        return ManifestEntry.Status.DELETED;
      case MODIFIED:
        // MODIFIED is the live state of a modified entry; surface as EXISTING for legacy consumers.
        return ManifestEntry.Status.EXISTING;
      default:
        throw new IllegalArgumentException("Unknown entry status: " + entryStatus);
    }
  }
}
