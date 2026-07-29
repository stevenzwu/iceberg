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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructProjection;

/**
 * Reader that reads a v4+ manifest file as {@link TrackedFile}s.
 *
 * <p>The same reader also exposes commit-oriented views over the underlying {@code content_entry}
 * rows via {@link #forData} and {@link #forDelete}: legacy {@link ManifestEntry entries},
 * colocated deletion vectors, data-file and DV changes, and raw rows. The scan Builder path and
 * the commit-side factories are independent — the constructor state each populates is disjoint.
 */
class V4ManifestReader extends CloseableGroup implements CloseableIterable<TrackedFile> {
  static final int SUPPORTED_FORMAT_VERSION = 4;

  private final InputFile file;

  // scan-side state; unused on the commit-side view path
  private final Schema readSchema;
  private final boolean includeAll;
  private final ScanMetrics scanMetrics;
  private final Map<Integer, Pair<Evaluator, StructProjection>> partitionFilters;

  // commit-side view state; unused on the scan path (null / -1)
  private final ManifestContent contentType;
  private final int defaultSpecId;
  private final Map<Integer, PartitionSpec> specsById;
  private final InheritableMetadata inheritableMetadata;

  private V4ManifestReader(
      InputFile file,
      Schema readSchema,
      Map<Integer, Pair<Evaluator, StructProjection>> partitionFilters,
      boolean includeAll,
      ScanMetrics scanMetrics) {
    this.file = file;
    this.readSchema = readSchema;
    this.partitionFilters = partitionFilters;
    this.includeAll = includeAll;
    this.scanMetrics = scanMetrics;
    this.contentType = null;
    this.defaultSpecId = -1;
    this.specsById = null;
    this.inheritableMetadata = null;
  }

  /** Constructor for commit-side views. Scan-side state is left unset. */
  private V4ManifestReader(
      InputFile file,
      ManifestContent contentType,
      int defaultSpecId,
      Map<Integer, PartitionSpec> specsById,
      InheritableMetadata inheritableMetadata) {
    this.file = file;
    this.readSchema = null;
    this.partitionFilters = Collections.emptyMap();
    this.includeAll = false;
    this.scanMetrics = ScanMetrics.noop();
    this.contentType = contentType;
    this.defaultSpecId = defaultSpecId;
    this.specsById = specsById;
    this.inheritableMetadata = inheritableMetadata;
  }

  static Builder builder(InputFile file, Map<Integer, PartitionSpec> specsById) {
    return new Builder(file, specsById);
  }

  /** Opens a content_entry reader for a data manifest (v4+ leaf). */
  static V4ManifestReader forData(
      InputFile file,
      int specId,
      Map<Integer, PartitionSpec> specsById,
      InheritableMetadata inheritableMetadata) {
    return new V4ManifestReader(
        file, ManifestContent.DATA, specId, specsById, inheritableMetadata);
  }

  /** Opens a content_entry reader for a delete manifest (v4+ leaf). */
  static V4ManifestReader forDelete(
      InputFile file,
      int specId,
      Map<Integer, PartitionSpec> specsById,
      InheritableMetadata inheritableMetadata) {
    return new V4ManifestReader(
        file, ManifestContent.DELETES, specId, specsById, inheritableMetadata);
  }

  /** Returns copies of the tracked files that match this reader's configured filters. */
  @Override
  public CloseableIterator<TrackedFile> iterator() {
    CloseableIterable<TrackedFile> entries = CloseableIterable.transform(open(), this::prepare);
    if (!partitionFilters.isEmpty()) {
      // manifests have no partition, so the partition filter cannot apply to them
      entries =
          CloseableIterable.filter(entries, entry -> isManifest(entry) || matchesPartition(entry));
    }

    if (!includeAll) {
      entries = CloseableIterable.filter(entries, entry -> entry.tracking().isLive());
    }

    return CloseableIterable.transform(entries, TrackedFile::copy).iterator();
  }

  private boolean matchesPartition(TrackedFile trackedFile) {
    Integer specId = trackedFile.specId();
    if (specId == null) {
      // a file without a spec is not partitioned and may match the filter
      return true;
    }

    Pair<Evaluator, StructProjection> partitionFilter = partitionFilters.get(specId);
    if (partitionFilter == null) {
      // the row filter does not project to a partition filter for this spec
      return true;
    }

    Evaluator evaluator = partitionFilter.first();
    StructProjection projection = partitionFilter.second();
    boolean matches = evaluator.eval(projection.wrap(trackedFile.partition()));
    if (!matches) {
      incrementSkipCount(trackedFile.contentType());
    }

    return matches;
  }

  private void incrementSkipCount(FileContent content) {
    switch (content) {
      case DATA:
        scanMetrics.skippedDataFiles().increment();
        break;
      case EQUALITY_DELETES:
        scanMetrics.skippedDeleteFiles().increment();
        break;
      case DATA_MANIFEST:
        scanMetrics.skippedDataManifests().increment();
        break;
      case DELETE_MANIFEST:
        scanMetrics.skippedDeleteManifests().increment();
        break;
      default:
        throw new UnsupportedOperationException("Unsupported content type: " + content);
    }
  }

  private CloseableIterable<TrackedFile> open() {
    FileFormat format = FileFormat.fromFileName(file.location());
    Preconditions.checkArgument(
        format != null, "Cannot determine format of manifest: %s", file.location());

    CloseableIterable<TrackedFile> reader =
        InternalData.read(format, file)
            .project(readSchema)
            .setRootType(TrackedFileStruct.class)
            .setCustomType(TrackedFile.TRACKING.fieldId(), TrackingStruct.class)
            .setCustomType(TrackedFile.DELETION_VECTOR.fieldId(), DeletionVectorStruct.class)
            .setCustomType(TrackedFile.MANIFEST_INFO.fieldId(), ManifestInfoStruct.class)
            .setCustomType(TrackedFile.PARTITION_ID, PartitionData.class)
            .reuseContainers()
            .build();
    addCloseable(reader);
    return reader;
  }

  private TrackedFile prepare(TrackedFile trackedFile) {
    Tracking tracking = trackedFile.tracking();
    // manifestLocation is not stored in the manifest; the reader fills it in
    if (tracking instanceof TrackingStruct) {
      ((TrackingStruct) tracking).setManifestLocation(file.location());
    }

    return trackedFile;
  }

  private static boolean isManifest(TrackedFile trackedFile) {
    FileContent content = trackedFile.contentType();
    return content == FileContent.DATA_MANIFEST || content == FileContent.DELETE_MANIFEST;
  }

  // -------------------------------------------------------------------------------------------
  // Commit-oriented views over the underlying content_entry rows.
  // -------------------------------------------------------------------------------------------

  /** Returns all entries (including deleted) as data manifest entries. */
  CloseableIterable<ManifestEntry<DataFile>> dataEntries() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read data entries from a delete manifest: %s",
        file.location());
    return readEntries();
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
   * Returns data-file changes encoded as {@code (v4+ status, DataFile)} pairs. Unlike {@link
   * #dataEntries()} which collapses REPLACED to DELETED and MODIFIED to EXISTING for legacy
   * consumers, this method surfaces the v4+ tracking status directly so callers can distinguish
   * data-file changes (ADDED, DELETED) from DV-state transitions (REPLACED, MODIFIED).
   */
  CloseableIterable<Pair<EntryStatus, DataFile>> dataFileChanges() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read data file changes from a delete manifest: %s",
        file.location());
    return readDataFileChanges();
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
    return readDVDeleteFiles();
  }

  /**
   * Returns colocated DV changes encoded as {@code (status, DeleteFile)} pairs, suitable for
   * computing per-snapshot delete-file deltas: ADDED DVs (ADDED and MODIFIED rows) and DELETED
   * DVs (REPLACED rows). EXISTING rows and rows without a DV are skipped.
   */
  CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> colocatedDVChanges() {
    Preconditions.checkArgument(
        contentType == ManifestContent.DATA,
        "Cannot read deletion vector changes from a delete manifest: %s",
        file.location());
    return readDVChanges();
  }

  /**
   * Returns the raw {@code content_entry} rows as defensive copies, preserving each entry's exact
   * status, colocated deletion vector, partition, and stats. Each row is copied because the row
   * iterable does not reuse containers for commit-side views but a copy is made regardless so
   * consumers may hold onto the row safely.
   */
  CloseableIterable<TrackedFileStruct> rawRows() {
    return CloseableIterable.transform(buildRows(), row -> (TrackedFileStruct) row.copy());
  }

  /**
   * Builds the raw content_entry row iterable for this manifest and registers it for close.
   * Shared by every commit-side view so the {@code content_entry} decode happens in one place.
   */
  private CloseableIterable<TrackedFileStruct> buildRows() {
    PartitionSpec defaultSpec = resolveDefaultSpec();
    Types.StructType statsType =
        StatsUtil.statsReadSchema(
            defaultSpec.schema(), TypeUtil.getProjectedIds(defaultSpec.schema()));
    Schema contentEntrySchema = buildContentEntrySchema(defaultSpec, statsType);
    // v4+ leaf manifests are always Parquet; match the content_entry writer's format.
    return openRows(
        FileFormat.PARQUET,
        contentEntrySchema,
        statsType,
        false /* copy rows defensively downstream */);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private <F extends ContentFile<F>> CloseableIterable<ManifestEntry<F>> readEntries() {
    // toManifestEntry fully materializes each row into a new ManifestEntry (fresh file, metrics,
    // and partition), so the row can be passed directly without a defensive copy.
    return (CloseableIterable<ManifestEntry<F>>)
        (CloseableIterable) CloseableIterable.transform(buildRows(), this::toManifestEntry);
  }

  private CloseableIterable<DeleteFile> readDVDeleteFiles() {
    CloseableIterable<DeleteFile> dvs =
        CloseableIterable.transform(
            buildRows(),
            row -> {
              TrackedFileStruct copy = (TrackedFileStruct) row.copy();
              if (!isLiveDataRowWithDV(copy)) {
                return null;
              }

              return toDVDeleteFile(copy);
            });

    return CloseableIterable.filter(dvs, dv -> dv != null);
  }

  private CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> readDVChanges() {
    CloseableIterable<Pair<ManifestEntry.Status, DeleteFile>> changes =
        CloseableIterable.transform(
            buildRows(),
            row -> {
              TrackedFileStruct copy = (TrackedFileStruct) row.copy();
              ManifestEntry.Status changeStatus = dvChangeStatus(copy);
              if (changeStatus == null) {
                return null;
              }

              return Pair.of(changeStatus, toDVDeleteFile(copy));
            });

    return CloseableIterable.filter(changes, p -> p != null);
  }

  private CloseableIterable<Pair<EntryStatus, DataFile>> readDataFileChanges() {
    CloseableIterable<Pair<EntryStatus, DataFile>> changes =
        CloseableIterable.transform(
            buildRows(),
            row -> {
              TrackedFileStruct copy = (TrackedFileStruct) row.copy();
              if (copy.contentType() != FileContent.DATA) {
                return null;
              }

              Tracking tracking = copy.tracking();
              if (tracking == null) {
                return null;
              }

              EntryStatus status = tracking.status();
              if (status != EntryStatus.ADDED && status != EntryStatus.DELETED) {
                return null;
              }

              Integer specId = copy.specId();
              PartitionSpec spec = specById(specId);
              if (spec == null) {
                spec = resolveDefaultSpec();
              }

              DataFile dataFile = toDataFile(copy, spec, tracking);
              // Apply InheritableMetadata so the returned DataFile carries the data/file sequence
              // numbers callers expect (BaseFile fields populated from the parent manifest's
              // sequence number for ADDED entries).
              GenericManifestEntry<DataFile> entry =
                  new GenericManifestEntry<>(spec.partitionType());
              ManifestEntry.Status manifestStatus = toManifestStatus(status);
              setEntry(
                  entry,
                  manifestStatus,
                  tracking.snapshotId(),
                  tracking.dataSequenceNumber(),
                  tracking.fileSequenceNumber(),
                  dataFile);
              inheritableMetadata.apply(entry);
              return Pair.of(status, entry.file());
            });

    return CloseableIterable.filter(changes, p -> p != null);
  }

  /**
   * Decodes {@code content_entry} rows for the given projection and registers the row iterable for
   * close. When {@code statsType} is non-null the per-column stats sub-structs are registered so
   * the projected {@code content_stats} column round-trips.
   */
  private CloseableIterable<TrackedFileStruct> openRows(
      FileFormat format, Schema projection, Types.StructType statsType, boolean reuseContainers) {
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

    if (reuseContainers) {
      builder.reuseContainers();
    }

    CloseableIterable<TrackedFileStruct> rows = builder.build();
    addCloseable(rows);
    return rows;
  }

  // Builds a GenericDeleteFile from a v4+ colocated DV row. Using GenericDeleteFile (a BaseFile)
  // rather than a lighter adapter lets InheritableMetadata propagate the dataSequenceNumber from
  // the parent manifest to the file — required for DeleteFileIndex's sequence-number checks.
  private DeleteFile toDVDeleteFile(TrackedFileStruct row) {
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

  private static boolean isLiveDataRowWithDV(TrackedFileStruct row) {
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
  private static ManifestEntry.Status dvChangeStatus(TrackedFileStruct row) {
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

  private Schema buildContentEntrySchema(PartitionSpec spec, Types.StructType statsType) {
    // v4+ leaf manifests encode partition tuples with the union partition type (a struct covering
    // every live spec's fields). Read with the same union so per-spec subsets land in the correct
    // positions; per-spec projection happens later in the projector. An empty union is projected
    // through EMPTY_PARTITION_PLACEHOLDER so a physical partition column reads null — this reader
    // can be invoked without the real spec (e.g. the deprecated Snapshot.addedDeleteFiles path).
    Types.StructType unionType =
        specsById != null && !specsById.isEmpty()
            ? Partitioning.unionPartitionTypes(specsById.values())
            : spec.partitionType();
    Types.StructType readPartitionType =
        TrackedFileWriter.emptyPartitionPlaceholderIfNeeded(unionType);
    return TrackedFile.schema(readPartitionType, statsType);
  }

  private ManifestEntry<?> toManifestEntry(TrackedFileStruct row) {
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
        content != null, "Invalid content_entry row: missing content_type in %s", file.location());

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

  private PartitionSpec specById(Integer specId) {
    if (specsById != null && specId != null) {
      return specsById.get(specId);
    }

    return null;
  }

  private DataFile toDataFile(TrackedFileStruct row, PartitionSpec spec, Tracking tracking) {
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

  private DeleteFile toEqualityDeleteFile(TrackedFileStruct row, PartitionSpec spec) {
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

  private static Metrics toMetrics(TrackedFileStruct row) {
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

  private static PartitionData toPartitionData(TrackedFileStruct row, PartitionSpec spec) {
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

  static class Builder {
    private final InputFile file;
    private final Types.StructType unionPartitionType;
    private final Map<Integer, PartitionSpec> specsById;
    private final Schema fullSchema;
    private Expression rowFilter = Expressions.alwaysTrue();
    private boolean caseSensitive = true;
    private boolean includeAll = false;
    private boolean scanPlanning = false;
    private Collection<String> columns = null;
    private Schema requestedProjection = null;
    private ScanMetrics scanMetrics = ScanMetrics.noop();

    private Builder(InputFile file, Map<Integer, PartitionSpec> specsById) {
      this.file = file;
      this.specsById = specsById;
      this.unionPartitionType = Partitioning.unionPartitionTypes(specsById.values());
      Schema base = TrackedFile.schema(unionPartitionType, Types.StructType.of());
      // the read schema carries row_position (via BASE_TYPE) so the reader can fill manifestPos
      this.fullSchema =
          TypeUtil.replaceFieldTypes(
              base, ImmutableMap.of(TrackedFile.TRACKING.fieldId(), TrackingStruct.BASE_TYPE));
    }

    /** Sets a filter; files that cannot match the expression are skipped. */
    Builder filter(Expression expr) {
      Preconditions.checkArgument(expr != null, "Invalid filter: null");
      this.rowFilter = expr;
      return this;
    }

    Builder caseSensitive(boolean isCaseSensitive) {
      this.caseSensitive = isCaseSensitive;
      return this;
    }

    /** Returns all entries without filtering by {@link Tracking#isLive() liveness}. */
    Builder includeAll() {
      this.includeAll = true;
      return this;
    }

    /** Configures the reader to select the minimal fields needed for scan planning. */
    Builder forScanPlanning() {
      Preconditions.checkState(
          columns == null && requestedProjection == null,
          "Cannot use forScanPlanning() with select(Collection<String>) or project(Schema)");
      this.scanPlanning = true;
      return this;
    }

    /** Selects columns to read by name; fields needed by the reader are always read. */
    Builder select(String... newColumns) {
      return select(Arrays.asList(newColumns));
    }

    /** Selects columns to read by name; fields needed by the reader are always read. */
    Builder select(Collection<String> newColumns) {
      Preconditions.checkArgument(newColumns != null, "Invalid columns: null");
      Preconditions.checkState(
          !scanPlanning, "Cannot use select(Collection<String>) with forScanPlanning()");
      Preconditions.checkState(
          requestedProjection == null,
          "Cannot select columns using both select(Collection<String>) and project(Schema)");
      this.columns = newColumns;
      return this;
    }

    /** Sets the exact schema to read; used in place of {@link #select(Collection)}. */
    Builder project(Schema newProjection) {
      Preconditions.checkState(!scanPlanning, "Cannot use project(Schema) with forScanPlanning()");
      Preconditions.checkState(
          columns == null,
          "Cannot select columns using both select(Collection<String>) and project(Schema)");
      this.requestedProjection = newProjection;
      return this;
    }

    Builder scanMetrics(ScanMetrics newScanMetrics) {
      Preconditions.checkArgument(newScanMetrics != null, "Invalid scan metrics: null");
      this.scanMetrics = newScanMetrics;
      return this;
    }

    V4ManifestReader build() {
      Map<Integer, Pair<Evaluator, StructProjection>> partitionFilters = Maps.newHashMap();
      if (rowFilter != Expressions.alwaysTrue() && !unionPartitionType.fields().isEmpty()) {
        for (PartitionSpec spec : specsById.values()) {
          Expression partFilter = Projections.inclusive(spec, caseSensitive).project(rowFilter);
          if (partFilter != Expressions.alwaysTrue()) {
            Evaluator evaluator = new Evaluator(spec.partitionType(), partFilter, caseSensitive);
            StructProjection projection =
                StructProjection.create(unionPartitionType, spec.partitionType());
            partitionFilters.put(spec.specId(), Pair.of(evaluator, projection));
          }
        }
      }

      boolean hasPartitionFilter = !partitionFilters.isEmpty();
      return new V4ManifestReader(
          file, readSchema(hasPartitionFilter), partitionFilters, includeAll, scanMetrics);
    }

    private Schema readSchema(boolean hasPartitionFilter) {
      if (scanPlanning) {
        // scan planning does not read the change-tracking fields omitted by SCAN_TYPE
        return TypeUtil.replaceFieldTypes(
            fullSchema, ImmutableMap.of(TrackedFile.TRACKING.fieldId(), TrackingStruct.SCAN_TYPE));
      }

      if (columns != null) {
        Schema selected =
            caseSensitive ? fullSchema.select(columns) : fullSchema.caseInsensitiveSelect(columns);
        return addRequiredColumns(selected, hasPartitionFilter);
      }

      if (requestedProjection != null) {
        return addRequiredColumns(requestedProjection, hasPartitionFilter);
      }

      return fullSchema;
    }

    private Schema addRequiredColumns(Schema projection, boolean hasPartitionFilter) {
      Set<Integer> projectedIds = Sets.newHashSet(TypeUtil.getProjectedIds(projection));

      // fields the reader consumes internally: status for liveness filtering, row_position for
      // manifestPos, and content type to distinguish entry kinds
      projectedIds.add(Tracking.STATUS.fieldId());
      projectedIds.add(MetadataColumns.ROW_POSITION.fieldId());
      projectedIds.add(TrackedFile.CONTENT_TYPE.fieldId());
      if (rowFilter != Expressions.alwaysTrue()) {
        // record_count is read when evaluating a filter against file metrics
        projectedIds.add(TrackedFile.RECORD_COUNT.fieldId());
      }

      // add the partition tuple only when it is needed to evaluate a partition filter
      if (hasPartitionFilter) {
        projectedIds.add(TrackedFile.SPEC_ID.fieldId());
        projectedIds.add(TrackedFile.PARTITION_ID);
        projectedIds.addAll(TypeUtil.getProjectedIds(unionPartitionType));
      }

      // project instead of select to preserve narrow struct projections from the caller
      return TypeUtil.project(fullSchema, projectedIds);
    }
  }
}
