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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Streaming roller over {@link TrackedFile} rows for the pool that owns the root manifest (the live
 * data pool). Opens a fresh {@link LeafManifestWriter} lazily on the first {@link #add}, streams
 * every row through it, and rolls to a new writer when the projected on-disk size ({@code
 * rowsInCurrentWriter × avgBytesPerEntry}) reaches the target.
 *
 * <p>Unlike {@link BufferedLeafManifestWriter} it never buffers rows in memory: the last open
 * writer is <em>promoted</em> to the snapshot's root manifest via {@link #promoteCurrentToRoot}, so
 * its already-written rows become direct rows in the root — one fewer file per commit than a
 * separate root writer. Promotion is the only terminal operation (the writer is never closed
 * without promoting), and returns an open {@link RootManifestWriter} the caller drives (appending
 * retirement tails as direct rows and leaf-manifest-entries for rolled/external leaves) and then
 * closes; ownership of the underlying writer transfers to the caller.
 *
 * <p>Because the promoted writer's file header carries {@code content: data} (from its {@link
 * LeafManifestWriter#forData} origin), it looks cosmetically like a leaf to tooling but reads
 * correctly as root through {@code RootManifestReader}, which does not consult file metadata for
 * dispatch.
 */
class StreamingLeafManifestWriter {

  private final Supplier<LeafManifestWriter> writerSupplier;
  private final long targetSizeBytes;
  private final long avgBytesPerEntry;
  private final List<ManifestFile> completedLeaves;

  private LeafManifestWriter currentWriter;
  private int rowsInCurrentWriter;
  private boolean promoted = false;

  /**
   * @param writerSupplier supplies a fresh {@link LeafManifestWriter} on each roll (and once more
   *     when the caller promotes without any rows added)
   * @param targetSizeBytes byte threshold at which the current writer closes as a leaf and a new
   *     one opens; must be positive
   * @param avgBytesPerEntry projected on-disk bytes per row used with {@code rowsInCurrentWriter}
   *     to decide when to roll; must be positive
   */
  StreamingLeafManifestWriter(
      Supplier<LeafManifestWriter> writerSupplier, long targetSizeBytes, long avgBytesPerEntry) {
    Preconditions.checkArgument(writerSupplier != null, "Invalid writer supplier: null");
    Preconditions.checkArgument(
        targetSizeBytes > 0, "targetSizeBytes must be positive: %s", targetSizeBytes);
    Preconditions.checkArgument(
        avgBytesPerEntry > 0, "avgBytesPerEntry must be positive: %s", avgBytesPerEntry);
    this.writerSupplier = writerSupplier;
    this.targetSizeBytes = targetSizeBytes;
    this.avgBytesPerEntry = avgBytesPerEntry;
    this.completedLeaves = Lists.newArrayList();
  }

  /**
   * Streams a row to the currently-open writer, opening a new one if none is open. If the projected
   * size crosses the target after this add, closes the current writer as a leaf so a fresh writer
   * is opened on the next add.
   */
  void add(TrackedFile row) {
    Preconditions.checkState(!promoted, "Cannot add after the writer has been promoted");
    Preconditions.checkNotNull(row, "TrackedFile row cannot be null");
    ensureWriterOpen();
    currentWriter.add(row);
    rowsInCurrentWriter++;
    if ((long) rowsInCurrentWriter * avgBytesPerEntry >= targetSizeBytes) {
      closeCurrentAsLeaf();
    }
  }

  /**
   * Promotes the still-open writer to root and returns it as an open {@link RootManifestWriter}:
   * the already-streamed rows remain as direct rows in the root, and the caller appends any
   * retirement tails ({@link RootManifestWriter#add}) and leaf-manifest-entries ({@link
   * RootManifestWriter#addManifestEntry}) before closing it. If no writer is currently open, opens
   * one on the spot to hold the promotion content. Promotion is terminal — ownership transfers to
   * the caller and this streaming writer must not be used further.
   *
   * @param snapshotId the committing snapshot id, used to resolve {@code UNASSIGNED_SEQ} on refs
   * @param sequenceNumber the committing sequence number
   * @param nextRowId the running first-row-id counter for freshly-written DATA manifest refs
   * @throws IllegalStateException if already promoted
   */
  RootManifestWriter promoteCurrentToRoot(long snapshotId, long sequenceNumber, Long nextRowId) {
    Preconditions.checkState(!promoted, "StreamingLeafManifestWriter already promoted");
    ensureWriterOpen();
    RootManifestWriter root = currentWriter.promoteToRoot(snapshotId, sequenceNumber, nextRowId);
    this.currentWriter = null;
    this.rowsInCurrentWriter = 0;
    this.promoted = true;
    return root;
  }

  /** Returns the leaf {@link ManifestFile}s rolled before promotion. */
  List<ManifestFile> completedLeaves() {
    Preconditions.checkState(promoted, "StreamingLeafManifestWriter is not promoted yet");
    return ImmutableList.copyOf(completedLeaves);
  }

  private void ensureWriterOpen() {
    if (currentWriter == null) {
      this.currentWriter = writerSupplier.get();
      this.rowsInCurrentWriter = 0;
    }
  }

  private void closeCurrentAsLeaf() {
    try {
      currentWriter.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close leaf writer", e);
    }

    completedLeaves.add(currentWriter.toManifestFile());
    this.currentWriter = null;
    this.rowsInCurrentWriter = 0;
  }
}
