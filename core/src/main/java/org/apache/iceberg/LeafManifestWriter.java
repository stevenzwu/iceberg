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
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * Writes a v4+ leaf data/delete manifest: {@code content_entry} rows for content files with
 * column-level stats. Maintains per-status counters and a partition summary from each row's {@link
 * TrackedFile#tracking() tracking} so {@link #toManifestFile()} can emit accurate record counts,
 * minimum sequence numbers, and REPLACED aggregates. Parallels {@link ManifestWriter} for v1–v3.
 *
 * <p>Callers construct fully-formed {@link TrackedFile} rows and hand them to {@link
 * #add(TrackedFile)}; the row's status, partition, colocated deletion vector, and column stats are
 * written exactly as given.
 *
 * <p>A still-open leaf writer can be {@link #promoteToRoot promoted} to a {@link
 * RootManifestWriter} — the adaptive-tree small-write optimization that turns the last leaf into
 * the snapshot's root.
 */
class LeafManifestWriter {
  private final TrackedFileWriter writer;
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
  private boolean closed = false;

  private LeafManifestWriter(
      TrackedFileWriter writer,
      EncryptionKeyMetadata keyMetadata,
      FileFormat format,
      int specId,
      Long snapshotId,
      Long firstRowId,
      ManifestContent content,
      PartitionSummary stats) {
    this.writer = writer;
    this.keyMetadata = keyMetadata;
    this.format = format;
    this.specId = specId;
    this.snapshotId = snapshotId;
    this.firstRowId = firstRowId;
    this.content = content;
    this.stats = stats;
  }

  // ---- Factories -------------------------------------------------------------

  /** Opens a writer for a v4+ leaf data manifest with the table's union partition type. */
  static LeafManifestWriter forData(
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

  /** Opens a writer for a v4+ leaf delete manifest with the table's union partition type. */
  static LeafManifestWriter forDelete(
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

  private static LeafManifestWriter forLeaf(
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
    Types.StructType partitionType = unionPartitionType;
    MetricsConfig metricsConfig = MetricsConfig.from(writerProperties, spec.schema(), null);

    FileAppender<StructLike> appender =
        newLeafAppender(format, out, spec, partitionType, metricsConfig, writerProperties, content);
    TrackedFileWriter writer = new TrackedFileWriter(out, appender);
    return new LeafManifestWriter(
        writer,
        keyMetadata,
        format,
        spec.specId(),
        snapshotId,
        firstRowId,
        content,
        new PartitionSummary(spec));
  }

  private static OutputFile outputFileForLeaf(
      FileFormat format, EncryptedOutputFile encryptedFile) {
    if (format == FileFormat.PARQUET
        && encryptedFile instanceof NativeEncryptionOutputFile nativeFile) {
      return nativeFile;
    }

    return encryptedFile.encryptingOutputFile();
  }

  // ---- Writes ----------------------------------------------------------------

  /**
   * Appends a {@link TrackedFile} row verbatim (its status, partition, colocated deletion vector,
   * and column stats are written exactly as given). Updates per-status counters and the partition
   * summary from the row's tracking; the caller owns row construction.
   */
  void add(TrackedFile row) {
    Preconditions.checkState(!closed, "Cannot add to a closed LeafManifestWriter");
    Preconditions.checkNotNull(row, "TrackedFile row cannot be null");
    updateCounters(row);
    writer.add(row);
  }

  Metrics metrics() {
    return writer.metrics();
  }

  long length() {
    return writer.length();
  }

  void close() throws IOException {
    if (!closed) {
      this.closed = true;
      writer.close();
    }
  }

  /**
   * Promotes this still-open leaf writer to a {@link RootManifestWriter} over the same appender:
   * its already-written rows become direct rows in the root. The leaf schema admits null-stats /
   * null-partition leaf-manifest-entry rows (its partition and content_stats columns are optional),
   * so the resulting root can carry leaf-manifest-entries. Leaf counters are abandoned — a root
   * manifest does not surface per-status leaf counts. The caller owns the returned writer's {@link
   * RootManifestWriter#close()}.
   */
  RootManifestWriter promoteToRoot(
      long commitSnapshotId, long commitSequenceNumber, Long nextRowId) {
    Preconditions.checkState(!closed, "Cannot promote a closed LeafManifestWriter");
    return RootManifestWriter.overPromotedLeaf(
        writer, commitSnapshotId, commitSequenceNumber, nextRowId);
  }

  /** Builds the leaf {@link ManifestFile} (format_version=4) for this writer. */
  ManifestFile toManifestFile() {
    Preconditions.checkState(closed, "Cannot build ManifestFile, writer is not closed");
    long minSeqNumber =
        minDataSequenceNumber != null ? minDataSequenceNumber : ManifestWriter.UNASSIGNED_SEQ;
    boolean hasReplacedOrModified = replacedFiles > 0 || modifiedFiles > 0;
    // MODIFIED entries are live (v4+ REPLACED/MODIFIED pairs); fold into existing counts so that
    // ManifestFilterManager.hasExistingFiles() remains correct. The on-disk record_count equals the
    // total number of rows appended (including statuses not surfaced via the per-status accessors).
    return new GenericManifestFile(
        writer.outputFile().location(),
        writer.length(),
        specId,
        content,
        ManifestWriter.UNASSIGNED_SEQ,
        minSeqNumber,
        snapshotId,
        stats.summaries(),
        leafKeyMetadataBuffer(),
        addedFiles,
        addedRows,
        existingFiles + modifiedFiles,
        existingRows + modifiedRows,
        deletedFiles,
        deletedRows,
        firstRowId,
        (long) entriesWritten,
        TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
        hasReplacedOrModified ? replacedFiles : null,
        hasReplacedOrModified ? replacedRows : null);
  }

  // ---- Counters --------------------------------------------------------------

  private void updateCounters(TrackedFile row) {
    Tracking tracking = row.tracking();
    EntryStatus status = tracking.status();
    long recordCount = row.recordCount();
    switch (status) {
      case ADDED:
        addedFiles += 1;
        addedRows += recordCount;
        break;
      case EXISTING:
        existingFiles += 1;
        existingRows += recordCount;
        break;
      case DELETED:
        deletedFiles += 1;
        deletedRows += recordCount;
        break;
      case REPLACED:
        // Terminal old-half of a v4 REPLACED/MODIFIED pair.
        replacedFiles += 1;
        replacedRows += recordCount;
        break;
      case MODIFIED:
        // Live new-half of a v4 REPLACED/MODIFIED pair.
        modifiedFiles += 1;
        modifiedRows += recordCount;
        break;
      default:
        throw new IllegalArgumentException("Unknown tracking status: " + status);
    }

    entriesWritten += 1;
    stats.update(row.partition());

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
    if (seqNum != null && (minDataSequenceNumber == null || seqNum < minDataSequenceNumber)) {
      this.minDataSequenceNumber = seqNum;
    }
  }

  private ByteBuffer leafKeyMetadataBuffer() {
    if (keyMetadata instanceof NativeEncryptionKeyMetadata nativeKeyMetadata
        && format == FileFormat.AVRO) {
      return nativeKeyMetadata.copyWithLength(writer.length()).buffer();
    } else if (keyMetadata != null) {
      return keyMetadata.buffer();
    }

    return null;
  }

  // ---- Appender factory ------------------------------------------------------

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
}
