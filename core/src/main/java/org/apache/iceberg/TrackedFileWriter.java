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
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;

/**
 * Writes v4+ {@code content_entry} Parquet files, the on-disk format shared by root manifests (the
 * v4+ replacement for the manifest list) and leaf data/delete manifests.
 *
 * <p>Callers construct fully-formed {@link TrackedFile} rows and hand them to {@link
 * #add(TrackedFile)}. The writer maintains per-status counters read from the row's {@link
 * TrackedFile#tracking() tracking()} so {@link #toManifestFile()} can emit accurate record counts,
 * minimum sequence numbers, and REPLACED aggregates.
 *
 * <p>A single instance serves one of two roles, chosen by the factory used to build it:
 *
 * <ul>
 *   <li><b>Root</b> ({@link #forRoot}) — each row is a manifest reference ({@code
 *       content_type=DATA_MANIFEST}/{@code DELETE_MANIFEST}) or (in Phase 3+) a direct content-file
 *       row. The partition column is the union of every live partition spec; manifest reference
 *       rows leave it null. Convenience overloads {@link #add(ManifestFile)} / {@link
 *       #add(ManifestFile, EntryStatus)} wrap a ManifestFile into a reference row, assigning
 *       sequence numbers and first-row-id where needed before appending.
 *   <li><b>Leaf</b> ({@link #forDataLeaf}/{@link #forDeleteLeaf}) — each row is a content-file
 *       entry with column stats. The row's tracking status drives per-status counters used by
 *       {@link #toManifestFile()}.
 * </ul>
 *
 * <p>The static schema helpers here ({@link #ROOT_CONTENT_STATS_TYPE}, {@link
 * #EMPTY_PARTITION_PLACEHOLDER}) are the shared schema contract, consumed by these writers and by
 * the readers ({@code RootManifestReader}, {@code V4ManifestReader}).
 */
class TrackedFileWriter implements FileAppender<TrackedFile> {
  /**
   * Content stats type for the root manifest. Root manifest entries do not carry column-level
   * stats, so a placeholder struct with a single dummy optional boolean field is used. Parquet
   * cannot encode an empty struct, so this placeholder is always written as null and ignored on
   * read.
   */
  static final Types.StructType ROOT_CONTENT_STATS_TYPE =
      Types.StructType.of(Types.NestedField.optional(99998, "_no_stats", Types.BooleanType.get()));

  /**
   * Single-field placeholder partition struct used as the partition <em>read</em> projection by
   * readers that may lack the real partition spec (see {@link #emptyPartitionPlaceholderIfNeeded}).
   * Field-id projection through this struct reads any physical partition column (partitioned tables
   * have one on their direct rows) as null; unpartitioned manifests carry no partition column and
   * also read null. It is never written to disk: writers use the table's real partition type, and
   * unpartitioned tables store no partition column (the empty type maps to {@link
   * org.apache.iceberg.types.Types.UnknownType} and is omitted).
   */
  static final Types.StructType EMPTY_PARTITION_PLACEHOLDER =
      Types.StructType.of(
          Types.NestedField.optional(99999, "_unpartitioned", Types.BooleanType.get()));

  /**
   * Returns the partition type to project when <em>reading</em>: the input if it has fields, or
   * {@link #EMPTY_PARTITION_PLACEHOLDER} when empty. Readers that may lack the real partition spec
   * use this so a physical partition column projects to null via field-id mismatch, and an absent
   * column (unpartitioned) also reads null. Writers do not use it: they pass the table's real
   * partition type, so unpartitioned tables store no partition column.
   */
  static Types.StructType emptyPartitionPlaceholderIfNeeded(Types.StructType partitionType) {
    return partitionType.fields().isEmpty() ? EMPTY_PARTITION_PLACEHOLDER : partitionType;
  }

  private final OutputFile outputFile;
  private final FileAppender<StructLike> appender;
  private final RootState root;
  private final LeafState leaf;
  private boolean closed = false;

  private TrackedFileWriter(
      OutputFile outputFile, FileAppender<StructLike> appender, RootState root, LeafState leaf) {
    this.outputFile = outputFile;
    this.appender = appender;
    this.root = root;
    this.leaf = leaf;
  }

  // ---- Factories -------------------------------------------------------------

  /**
   * Opens a writer for a v4+ root manifest.
   *
   * @param partitionType the union of every live partition spec's partition type; the writer wraps
   *     an empty union in {@link #EMPTY_PARTITION_PLACEHOLDER}
   * @param contentStatsType the content_stats struct type the writer encodes; use {@link
   *     #ROOT_CONTENT_STATS_TYPE} for reference-only root manifests
   */
  static TrackedFileWriter forRoot(
      OutputFile file,
      EncryptionManager encryptionManager,
      long snapshotId,
      Long parentSnapshotId,
      long sequenceNumber,
      Long snapshotFirstRowId,
      Types.StructType partitionType,
      Types.StructType contentStatsType) {
    StandardEncryptionManager standardEncryptionManager = null;
    OutputFile out = file;
    NativeEncryptionKeyMetadata keyMetadata = null;
    if (encryptionManager instanceof StandardEncryptionManager) {
      standardEncryptionManager = (StandardEncryptionManager) encryptionManager;
      EncryptedOutputFile encryptedFile = standardEncryptionManager.encrypt(file);
      if (encryptedFile instanceof NativeEncryptionOutputFile) {
        out = (NativeEncryptionOutputFile) encryptedFile;
      } else {
        out = encryptedFile.encryptingOutputFile();
      }

      keyMetadata =
          encryptedFile.keyMetadata() instanceof NativeEncryptionKeyMetadata
              ? (NativeEncryptionKeyMetadata) encryptedFile.keyMetadata()
              : null;
    }

    FileAppender<StructLike> appender =
        newRootAppender(
            out,
            snapshotId,
            parentSnapshotId,
            sequenceNumber,
            emptyPartitionPlaceholderIfNeeded(partitionType),
            contentStatsType);
    RootState state =
        new RootState(
            standardEncryptionManager, keyMetadata, snapshotId, sequenceNumber, snapshotFirstRowId);
    return new TrackedFileWriter(out, appender, state, null);
  }

  /** Opens a writer for a v4+ leaf data manifest. */
  static TrackedFileWriter forDataLeaf(
      PartitionSpec spec,
      Types.StructType unionPartitionType,
      EncryptedOutputFile file,
      Long snapshotId,
      Long firstRowId,
      Map<String, String> writerProperties) {
    return forLeaf(
        spec,
        unionPartitionType,
        file,
        snapshotId,
        firstRowId,
        writerProperties,
        ManifestContent.DATA);
  }

  /** Opens a writer for a v4+ leaf delete manifest. */
  static TrackedFileWriter forDeleteLeaf(
      PartitionSpec spec,
      Types.StructType unionPartitionType,
      EncryptedOutputFile file,
      Long snapshotId,
      Map<String, String> writerProperties) {
    return forLeaf(
        spec,
        unionPartitionType,
        file,
        snapshotId,
        null,
        writerProperties,
        ManifestContent.DELETES);
  }

  private static TrackedFileWriter forLeaf(
      PartitionSpec spec,
      Types.StructType unionPartitionType,
      EncryptedOutputFile file,
      Long snapshotId,
      Long firstRowId,
      Map<String, String> writerProperties,
      ManifestContent content) {
    FileFormat format = FileFormat.fromFileName(file.encryptingOutputFile().location());
    OutputFile out = outputFileForLeaf(format, file);
    EncryptionKeyMetadata keyMetadata = file.keyMetadata();
    Types.StructType partitionType = emptyPartitionPlaceholderIfNeeded(unionPartitionType);
    MetricsConfig metricsConfig = MetricsConfig.from(writerProperties, spec.schema(), null);

    FileAppender<StructLike> appender =
        newLeafAppender(format, out, spec, partitionType, metricsConfig, writerProperties, content);
    LeafState state =
        new LeafState(
            keyMetadata,
            format,
            spec.specId(),
            snapshotId,
            firstRowId,
            content,
            new PartitionSummary(spec));
    return new TrackedFileWriter(out, appender, null, state);
  }

  private static OutputFile outputFileForLeaf(
      FileFormat format, EncryptedOutputFile encryptedFile) {
    if (format == FileFormat.PARQUET
        && encryptedFile instanceof NativeEncryptionOutputFile nativeFile) {
      return nativeFile;
    }

    return encryptedFile.encryptingOutputFile();
  }

  // ---- FileAppender<TrackedFile> contract ------------------------------------

  /**
   * Appends a {@link TrackedFile} row verbatim (its status, partition, colocated deletion vector,
   * and column stats are written exactly as given). Updates per-status counters and the partition
   * summary from the row's tracking; the caller owns row construction.
   *
   * <p>All TrackedFile implementations in this package also implement {@link StructLike}; the row
   * is cast and handed to the underlying Parquet appender directly.
   */
  @Override
  public void add(TrackedFile row) {
    Preconditions.checkNotNull(row, "TrackedFile row cannot be null");
    if (leaf != null) {
      updateLeafCounters(row);
    }
    // For the root role there is no per-status accounting; the row is appended as given.
    appender.add((StructLike) row);
  }

  @Override
  public Metrics metrics() {
    return appender.metrics();
  }

  @Override
  public long length() {
    return appender.length();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      this.closed = true;
      appender.close();
    }
  }

  // ---- Root role convenience -------------------------------------------------

  /**
   * Adds a manifest reference entry with {@link EntryStatus#ADDED}. Root role only. The output's
   * {@code format_version} is read from {@link ManifestFile#formatVersion()}: producers of v4+ leaf
   * manifests set it to {@code 4}; legacy v1-v3 manifests carried over during a v3-to-v4 upgrade
   * default to {@code 0}.
   */
  void add(ManifestFile manifest) {
    addManifestEntry(manifest, EntryStatus.ADDED);
  }

  /**
   * Adds a manifest reference entry with an explicit entry status. Root role only. Use {@link
   * EntryStatus#EXISTING} for manifests carried over unchanged from the previous snapshot, and
   * {@link EntryStatus#ADDED} for manifests newly written in this snapshot.
   */
  void add(ManifestFile manifest, EntryStatus status) {
    addManifestEntry(manifest, status);
  }

  /**
   * Convenience method to add all manifests from an iterable. Renamed from {@code addAll} to avoid
   * a type-erasure clash with {@link FileAppender#addAll(Iterable)}.
   */
  void addAllManifests(Iterable<ManifestFile> manifests) {
    for (ManifestFile manifest : manifests) {
      add(manifest);
    }
  }

  private void addManifestEntry(ManifestFile manifest, EntryStatus status) {
    Preconditions.checkState(root != null, "add(ManifestFile) is only supported for the root role");
    ManifestFile resolved = assignSequenceNumber(manifest);
    Long firstRowId = resolveFirstRowId(resolved);
    root.trackedFile.wrap(resolved, status, firstRowId);
    appender.add((StructLike) root.trackedFile);
  }

  /**
   * Resolves {@code UNASSIGNED_SEQ} on a freshly written leaf manifest so the root manifest entry
   * sees concrete sequence numbers.
   */
  private ManifestFile assignSequenceNumber(ManifestFile manifest) {
    long seq = manifest.sequenceNumber();
    long minSeq = manifest.minSequenceNumber();
    if (seq != ManifestWriter.UNASSIGNED_SEQ && minSeq != ManifestWriter.UNASSIGNED_SEQ) {
      return manifest;
    }

    Preconditions.checkState(
        manifest.snapshotId() != null && manifest.snapshotId() == root.commitSnapshotId,
        "Found unassigned sequence number for a manifest from snapshot: %s",
        manifest.snapshotId());

    long resolvedSeq = seq == ManifestWriter.UNASSIGNED_SEQ ? root.commitSequenceNumber : seq;
    long resolvedMinSeq =
        minSeq == ManifestWriter.UNASSIGNED_SEQ ? root.commitSequenceNumber : minSeq;
    return GenericManifestFile.copyOf(manifest)
        .withSequenceNumbers(resolvedSeq, resolvedMinSeq)
        .build();
  }

  private Long resolveFirstRowId(ManifestFile manifest) {
    if (manifest.content() != ManifestContent.DATA) {
      return null;
    }

    if (manifest.firstRowId() != null) {
      return manifest.firstRowId();
    }

    Preconditions.checkState(
        root.nextRowId != null,
        "Cannot assign first-row-id for DATA manifest without a snapshot first-row-id: %s",
        manifest.path());
    long assigned = root.nextRowId;
    long existingRows = manifest.existingRowsCount() != null ? manifest.existingRowsCount() : 0L;
    long addedRows = manifest.addedRowsCount() != null ? manifest.addedRowsCount() : 0L;
    root.nextRowId = assigned + existingRows + addedRows;
    return assigned;
  }

  /** Returns metadata about this root manifest file so callers can build a snapshot referring to it. */
  ManifestListFile toRootManifestFile() {
    Preconditions.checkState(
        root != null, "toRootManifestFile is only supported for the root role");
    if (root.keyMetadata != null && root.keyMetadata.encryptionKey() != null) {
      String keyId =
          root.standardEncryptionManager.addManifestListKeyMetadata(
              root.keyMetadata.copyWithLength(appender.length()));
      return new BaseManifestListFile(outputFile.location(), keyId);
    } else {
      return new BaseManifestListFile(outputFile.location(), null);
    }
  }

  // ---- Leaf role -------------------------------------------------------------

  /**
   * Updates the leaf-role counters and partition summary from the row's tracking. Called from
   * {@link #add(TrackedFile)} before the row is appended to the underlying file.
   */
  private void updateLeafCounters(TrackedFile row) {
    Tracking tracking = row.tracking();
    EntryStatus status = tracking.status();
    long recordCount = row.recordCount();
    switch (status) {
      case ADDED:
        leaf.addedFiles += 1;
        leaf.addedRows += recordCount;
        break;
      case EXISTING:
        leaf.existingFiles += 1;
        leaf.existingRows += recordCount;
        break;
      case DELETED:
        leaf.deletedFiles += 1;
        leaf.deletedRows += recordCount;
        break;
      case REPLACED:
        // Terminal old-half of a v4 REPLACED/MODIFIED pair. Populated by later phases; the counter
        // is present here so the accumulator (Phase 3) can dissolve leaves that already carry such
        // rows without breaking toManifestFile() math.
        leaf.replacedFiles += 1;
        leaf.replacedRows += recordCount;
        break;
      case MODIFIED:
        // Live new-half of a v4 REPLACED/MODIFIED pair. Populated by later phases (see REPLACED).
        leaf.modifiedFiles += 1;
        leaf.modifiedRows += recordCount;
        break;
      default:
        throw new IllegalArgumentException("Unknown tracking status: " + status);
    }

    leaf.entriesWritten += 1;
    leaf.stats.update(row.partition());

    // Live rows (ADDED/EXISTING/MODIFIED) update the min data sequence number. ADDED rows carry
    // null dataSequenceNumber (assigned at commit), so they contribute nothing until commit.
    if (isLive(status)) {
      updateMinDataSequenceNumber(tracking.dataSequenceNumber());
    }
  }

  private static boolean isLive(EntryStatus status) {
    return status == EntryStatus.ADDED
        || status == EntryStatus.EXISTING
        || status == EntryStatus.MODIFIED;
  }

  private void updateMinDataSequenceNumber(Long seqNum) {
    if (seqNum != null
        && (leaf.minDataSequenceNumber == null || seqNum < leaf.minDataSequenceNumber)) {
      leaf.minDataSequenceNumber = seqNum;
    }
  }

  /** Builds the leaf {@link ManifestFile} (format_version=4) for this writer. Leaf role only. */
  ManifestFile toManifestFile() {
    Preconditions.checkState(leaf != null, "toManifestFile is only supported for a leaf role");
    Preconditions.checkState(closed, "Cannot build ManifestFile, writer is not closed");
    long minSeqNumber =
        leaf.minDataSequenceNumber != null
            ? leaf.minDataSequenceNumber
            : ManifestWriter.UNASSIGNED_SEQ;
    boolean hasReplacedOrModified = leaf.replacedFiles > 0 || leaf.modifiedFiles > 0;
    // MODIFIED entries are live (v4+ REPLACED/MODIFIED pairs); fold into existing counts so that
    // ManifestFilterManager.hasExistingFiles() remains correct. The on-disk record_count equals the
    // total number of rows appended (including statuses not surfaced via the per-status accessors).
    return new GenericManifestFile(
        outputFile.location(),
        appender.length(),
        leaf.specId,
        leaf.content,
        ManifestWriter.UNASSIGNED_SEQ,
        minSeqNumber,
        leaf.snapshotId,
        leaf.stats.summaries(),
        leafKeyMetadataBuffer(),
        leaf.addedFiles,
        leaf.addedRows,
        leaf.existingFiles + leaf.modifiedFiles,
        leaf.existingRows + leaf.modifiedRows,
        leaf.deletedFiles,
        leaf.deletedRows,
        leaf.firstRowId,
        (long) leaf.entriesWritten,
        TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
        hasReplacedOrModified ? leaf.replacedFiles : null,
        hasReplacedOrModified ? leaf.replacedRows : null);
  }

  private ByteBuffer leafKeyMetadataBuffer() {
    EncryptionKeyMetadata keyMetadata = leaf.keyMetadata;
    if (keyMetadata instanceof NativeEncryptionKeyMetadata nativeKeyMetadata
        && leaf.format == FileFormat.AVRO) {
      return nativeKeyMetadata.copyWithLength(appender.length()).buffer();
    } else if (keyMetadata != null) {
      return keyMetadata.buffer();
    }

    return null;
  }

  // ---- Appender factories ----------------------------------------------------

  private static FileAppender<StructLike> newRootAppender(
      OutputFile file,
      long snapshotId,
      Long parentSnapshotId,
      long sequenceNumber,
      Types.StructType partitionType,
      Types.StructType contentStatsType) {
    Schema contentEntrySchema = TrackedFile.schema(partitionType, contentStatsType);
    try {
      return InternalData.write(FileFormat.PARQUET, file)
          .schema(contentEntrySchema)
          .named("content_entry")
          .meta(
              ImmutableMap.of(
                  "snapshot-id", String.valueOf(snapshotId),
                  "parent-snapshot-id", String.valueOf(parentSnapshotId),
                  "sequence-number", String.valueOf(sequenceNumber),
                  "format-version", "4",
                  "content", "root-manifest"))
          .set("iceberg.parquet.materialize-empty-file", "true")
          .overwrite()
          .build();
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to create root manifest writer for path: %s", file.location());
    }
  }

  private static FileAppender<StructLike> newLeafAppender(
      FileFormat format,
      OutputFile file,
      PartitionSpec spec,
      Types.StructType partitionType,
      MetricsConfig metricsConfig,
      Map<String, String> writerProperties,
      ManifestContent content) {
    Schema contentEntrySchema =
        TrackedFile.schema(partitionType, StatsUtil.statsWriteSchema(spec.schema(), metricsConfig));
    try {
      return InternalData.write(format, file)
          .schema(contentEntrySchema)
          .named("content_entry")
          .meta("schema", SchemaParser.toJson(spec.schema()))
          .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
          .meta("partition-spec-id", String.valueOf(spec.specId()))
          .meta("format-version", "4")
          .meta("content", content == ManifestContent.DATA ? "data" : "deletes")
          .set(writerProperties)
          .overwrite()
          .build();
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to create manifest writer for path: %s", file.location());
    }
  }

  // ---- State ------------------------------------------------------------------

  /** State for the root role: encryption bits, the reference wrapper, and the first-row-id counter. */
  private static final class RootState {
    private final StandardEncryptionManager standardEncryptionManager;
    private final NativeEncryptionKeyMetadata keyMetadata;
    private final TrackedFileAdapters.ManifestTrackedFile trackedFile =
        TrackedFileAdapters.forManifestReference();
    private final long commitSnapshotId;
    private final long commitSequenceNumber;
    private Long nextRowId;

    private RootState(
        StandardEncryptionManager standardEncryptionManager,
        NativeEncryptionKeyMetadata keyMetadata,
        long commitSnapshotId,
        long commitSequenceNumber,
        Long nextRowId) {
      this.standardEncryptionManager = standardEncryptionManager;
      this.keyMetadata = keyMetadata;
      this.commitSnapshotId = commitSnapshotId;
      this.commitSequenceNumber = commitSequenceNumber;
      this.nextRowId = nextRowId;
    }
  }

  /** State for a leaf role: partition summary and per-status counters. */
  private static final class LeafState {
    private final EncryptionKeyMetadata keyMetadata;
    private final FileFormat format;
    private final int specId;
    private final Long snapshotId;
    private final Long firstRowId;
    private final ManifestContent content;
    private final PartitionSummary stats;

    private int addedFiles = 0;
    private long addedRows = 0L;
    private int existingFiles = 0;
    private long existingRows = 0L;
    private int deletedFiles = 0;
    private long deletedRows = 0L;
    private int entriesWritten = 0;
    private int replacedFiles = 0;
    private long replacedRows = 0L;
    private int modifiedFiles = 0;
    private long modifiedRows = 0L;
    private Long minDataSequenceNumber = null;

    private LeafState(
        EncryptionKeyMetadata keyMetadata,
        FileFormat format,
        int specId,
        Long snapshotId,
        Long firstRowId,
        ManifestContent content,
        PartitionSummary stats) {
      this.keyMetadata = keyMetadata;
      this.format = format;
      this.specId = specId;
      this.snapshotId = snapshotId;
      this.firstRowId = firstRowId;
      this.content = content;
      this.stats = stats;
    }
  }
}
