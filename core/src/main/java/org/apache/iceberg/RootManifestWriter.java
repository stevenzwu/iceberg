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

import java.io.Closeable;
import java.io.IOException;
import org.apache.iceberg.encryption.EncryptedOutputFile;
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
 * Writes a v4+ root manifest — the replacement for the manifest list. Each row is either a
 * leaf-manifest-entry (a reference to a leaf data/delete manifest, {@code
 * content_type=DATA_MANIFEST} / {@code DELETE_MANIFEST}) or a direct content-file row (the
 * adaptive-tree small-write optimization). Parallels {@link ManifestListWriter} for v1–v3,
 * generalized to the {@link TrackedFile} {@code content_entry} schema.
 *
 * <p>Two ways to open one:
 *
 * <ul>
 *   <li>{@link #create} — a fresh root file with its own manifest-list encryption key metadata.
 *   <li>{@link LeafManifestWriter#promoteToRoot} — reuses a still-open leaf data-manifest writer's
 *       appender as the root, so its already-written rows become direct rows in the root (one fewer
 *       file per commit). Promoted roots carry no manifest-list encryption key (future work).
 * </ul>
 */
class RootManifestWriter implements Closeable {
  private final TrackedFileWriter writer;
  private final TrackedFileAdapters.ManifestTrackedFile refWrapper =
      TrackedFileAdapters.forManifestReference();
  private final StandardEncryptionManager standardEncryptionManager;
  private final NativeEncryptionKeyMetadata keyMetadata;
  private final long commitSnapshotId;
  private final long commitSequenceNumber;
  private Long nextRowId;
  private boolean closed = false;

  private RootManifestWriter(
      TrackedFileWriter writer,
      StandardEncryptionManager standardEncryptionManager,
      NativeEncryptionKeyMetadata keyMetadata,
      long commitSnapshotId,
      long commitSequenceNumber,
      Long nextRowId) {
    this.writer = writer;
    this.standardEncryptionManager = standardEncryptionManager;
    this.keyMetadata = keyMetadata;
    this.commitSnapshotId = commitSnapshotId;
    this.commitSequenceNumber = commitSequenceNumber;
    this.nextRowId = nextRowId;
  }

  // ---- Factories -------------------------------------------------------------

  /**
   * Opens a fresh v4+ root manifest with its own encryption key metadata.
   *
   * @param partitionType the union of every live partition spec's partition type; wrapped in the
   *     empty-partition placeholder when empty
   * @param contentStatsType the content_stats struct type the writer encodes; use {@link
   *     TrackedFileWriter#ROOT_CONTENT_STATS_TYPE} for reference-only root manifests
   */
  static RootManifestWriter create(
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
            out, snapshotId, parentSnapshotId, sequenceNumber, partitionType, contentStatsType);
    TrackedFileWriter writer = new TrackedFileWriter(out, appender);
    return new RootManifestWriter(
        writer,
        standardEncryptionManager,
        keyMetadata,
        snapshotId,
        sequenceNumber,
        snapshotFirstRowId);
  }

  /**
   * Wraps a still-open leaf writer's appender as the root (promotion). The leaf's already-written
   * rows remain as direct rows in the root; no manifest-list encryption key is assigned.
   */
  static RootManifestWriter overPromotedLeaf(
      TrackedFileWriter openLeafWriter,
      long commitSnapshotId,
      long commitSequenceNumber,
      Long nextRowId) {
    return new RootManifestWriter(
        openLeafWriter, null, null, commitSnapshotId, commitSequenceNumber, nextRowId);
  }

  // ---- Writes ----------------------------------------------------------------

  /** Appends a direct content-file row verbatim (the small-write optimization). */
  void add(TrackedFile row) {
    Preconditions.checkState(!closed, "Cannot add to a closed RootManifestWriter");
    Preconditions.checkNotNull(row, "TrackedFile row cannot be null");
    writer.add(row);
  }

  /**
   * Appends a leaf-manifest-entry referencing the given leaf manifest. Resolves {@link
   * ManifestWriter#UNASSIGNED_SEQ} against this commit's sequence number and assigns a first-row-id
   * to freshly-written DATA manifests that lack one, advancing the running counter.
   */
  void addManifestEntry(ManifestFile manifest, EntryStatus status) {
    Preconditions.checkState(!closed, "Cannot add to a closed RootManifestWriter");
    ManifestFile resolved = assignSequenceNumber(manifest);
    Long firstRowId = resolveFirstRowId(resolved);
    refWrapper.wrap(resolved, status, firstRowId);
    writer.add(refWrapper);
  }

  Metrics metrics() {
    return writer.metrics();
  }

  long length() {
    return writer.length();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      this.closed = true;
      writer.close();
    }
  }

  /**
   * Returns metadata about this root manifest file so callers can build a snapshot referring to it.
   * The writer must be closed first.
   */
  SnapshotFile toSnapshotFile() {
    Preconditions.checkState(closed, "Cannot build SnapshotFile, writer is not closed");
    if (keyMetadata != null && keyMetadata.encryptionKey() != null) {
      String keyId =
          standardEncryptionManager.addManifestListKeyMetadata(
              keyMetadata.copyWithLength(writer.length()));
      return new BaseSnapshotFile(writer.outputFile().location(), keyId);
    } else {
      return new BaseSnapshotFile(writer.outputFile().location(), null);
    }
  }

  // ---- Sequence-number / first-row-id resolution -----------------------------

  private ManifestFile assignSequenceNumber(ManifestFile manifest) {
    long seq = manifest.sequenceNumber();
    long minSeq = manifest.minSequenceNumber();
    if (seq != ManifestWriter.UNASSIGNED_SEQ && minSeq != ManifestWriter.UNASSIGNED_SEQ) {
      return manifest;
    }

    Preconditions.checkState(
        manifest.snapshotId() != null && manifest.snapshotId() == commitSnapshotId,
        "Found unassigned sequence number for a manifest from snapshot: %s",
        manifest.snapshotId());

    long resolvedSeq = seq == ManifestWriter.UNASSIGNED_SEQ ? commitSequenceNumber : seq;
    long resolvedMinSeq = minSeq == ManifestWriter.UNASSIGNED_SEQ ? commitSequenceNumber : minSeq;
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
        nextRowId != null,
        "Cannot assign first-row-id for DATA manifest without a snapshot first-row-id: %s",
        manifest.path());
    long assigned = nextRowId;
    long existingRows = manifest.existingRowsCount() != null ? manifest.existingRowsCount() : 0L;
    long addedRows = manifest.addedRowsCount() != null ? manifest.addedRowsCount() : 0L;
    this.nextRowId = assigned + existingRows + addedRows;
    return assigned;
  }

  // ---- Appender factory ------------------------------------------------------

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
}
