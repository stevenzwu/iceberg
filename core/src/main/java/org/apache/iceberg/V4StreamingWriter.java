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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;

/**
 * Streaming writer for the v4 adaptive live-data pool. Opens a fresh {@link TrackedFileWriter}
 * lazily on the first {@link #add}, streams every row through it, and rolls to a new writer when
 * the projected on-disk size ({@code rowsInCurrentWriter × avgBytesPerEntry}) reaches the target.
 *
 * <p>Two close paths, mutually exclusive:
 *
 * <ul>
 *   <li>{@link #promoteCurrentToRoot} — the still-open writer receives caller-supplied direct rows
 *       (retirement / eq-delete pool tails) and manifest-reference rows (leaf-manifest-entries for
 *       previously-rolled leaves and any external leaves) before closing. The resulting file is
 *       the snapshot's root manifest file — one fewer file per commit than the "separate root
 *       writer" approach. If nothing was added before promotion, a writer is opened on the spot
 *       just to hold the promotion content.
 *   <li>{@link #close} — the still-open writer closes as a final leaf. Suitable for producer paths
 *       that do not need a promoted root (currently unused by the accumulator, kept for
 *       symmetry with {@link RollingTrackedFileWriter#close()} so a future caller can consume
 *       leaves without promotion).
 * </ul>
 *
 * <p>Rolled leaves surface via {@link #completedLeaves()}. Because the promoted writer's file
 * header carries {@code content: data-manifest} (from its {@link TrackedFileWriter#forDataLeaf}
 * origin), it looks cosmetically like a leaf to tooling but reads correctly as root through
 * {@link RootManifestReader}, which does not consult file metadata for dispatch.
 */
class V4StreamingWriter implements Closeable {

  private final Supplier<TrackedFileWriter> writerSupplier;
  private final long targetSizeBytes;
  private final long avgBytesPerEntry;
  private final List<ManifestFile> completedLeaves;

  private TrackedFileWriter currentWriter;
  private int rowsInCurrentWriter;
  private boolean closed = false;
  private ManifestFile promotedRoot;

  /**
   * @param writerSupplier supplies a fresh {@link TrackedFileWriter} on each roll (and once more
   *     when the caller promotes without any rows added)
   * @param targetSizeBytes byte threshold at which the current writer closes as a leaf and a new
   *     one opens; must be positive
   * @param avgBytesPerEntry projected on-disk bytes per row used with {@code rowsInCurrentWriter}
   *     to decide when to roll; must be positive
   */
  V4StreamingWriter(
      Supplier<TrackedFileWriter> writerSupplier, long targetSizeBytes, long avgBytesPerEntry) {
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
   * Streams a row to the currently-open writer, opening a new one if none is open. If the
   * projected size crosses the target after this add, closes the current writer as a leaf so a
   * fresh writer is opened on the next add.
   */
  void add(TrackedFile row) {
    Preconditions.checkState(!closed, "Cannot add to a closed V4StreamingWriter");
    Preconditions.checkNotNull(row, "TrackedFile row cannot be null");
    ensureWriterOpen();
    currentWriter.add(row);
    rowsInCurrentWriter++;
    if ((long) rowsInCurrentWriter * avgBytesPerEntry >= targetSizeBytes) {
      closeCurrentAsLeaf();
    }
  }

  /**
   * Promotes the still-open writer to root: appends {@code extraDirectRows} (typically retirement
   * / eq-delete pool tails, which the streaming live pool did not receive as row adds) and
   * {@code leafReferences} (leaf-manifest-entries for previously-rolled leaves and any external
   * leaves the caller wants referenced), then closes the writer as the root manifest file. If no
   * writer is currently open, opens one on the spot to hold the promotion content.
   *
   * <p>The returned {@link ManifestFile}'s {@code location} and {@code fileSizeInBytes} are the
   * root manifest's path and size for {@code Snapshot.rootManifestLocation}. Its leaf-shaped
   * counts and partition summary reflect the writer's leaf-role bookkeeping and are not
   * semantically meaningful for a root manifest — callers should not consume them.
   *
   * @throws IllegalStateException if the writer is already closed
   */
  ManifestFile promoteCurrentToRoot(
      Iterable<TrackedFile> extraDirectRows,
      Iterable<Pair<ManifestFile, EntryStatus>> leafReferences,
      TrackedFileWriter.RootState refState) {
    Preconditions.checkState(!closed, "V4StreamingWriter already closed");
    Preconditions.checkArgument(refState != null, "Invalid RootState: null");
    ensureWriterOpen();
    for (TrackedFile row : extraDirectRows) {
      currentWriter.add(row);
    }
    for (Pair<ManifestFile, EntryStatus> ref : leafReferences) {
      currentWriter.addManifestEntry(ref.first(), ref.second(), refState);
    }
    try {
      currentWriter.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close promoted-root writer", e);
    }
    this.promotedRoot = currentWriter.toManifestFile();
    this.currentWriter = null;
    this.rowsInCurrentWriter = 0;
    this.closed = true;
    return promotedRoot;
  }

  /**
   * Closes without promotion: any still-open writer closes as a final leaf. Idempotent. Mutually
   * exclusive with {@link #promoteCurrentToRoot}.
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (currentWriter != null) {
      closeCurrentAsLeaf();
    }
    this.closed = true;
  }

  /** Returns the leaf {@link ManifestFile}s rolled before promotion / final close. */
  List<ManifestFile> completedLeaves() {
    Preconditions.checkState(closed, "V4StreamingWriter is not closed yet");
    return ImmutableList.copyOf(completedLeaves);
  }

  /**
   * Snapshots the leaves rolled so far <em>before</em> close/promotion. Used by
   * {@link V4CommitAccumulator#close} to build the leaf-manifest-entry refs it hands into
   * {@link #promoteCurrentToRoot} in the same call — the writer can't be closed first because
   * promotion is the close.
   */
  List<ManifestFile> peekRolledLeaves() {
    Preconditions.checkState(!closed, "V4StreamingWriter is already closed");
    return ImmutableList.copyOf(completedLeaves);
  }

  /**
   * Returns the promoted-root {@link ManifestFile} if {@link #promoteCurrentToRoot} was used, or
   * null if the writer closed via {@link #close()} without promotion.
   */
  ManifestFile promotedRoot() {
    Preconditions.checkState(closed, "V4StreamingWriter is not closed yet");
    return promotedRoot;
  }

  private void ensureWriterOpen() {
    if (currentWriter == null) {
      currentWriter = writerSupplier.get();
      rowsInCurrentWriter = 0;
    }
  }

  private void closeCurrentAsLeaf() {
    try {
      currentWriter.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close leaf writer", e);
    }
    completedLeaves.add(currentWriter.toManifestFile());
    currentWriter = null;
    rowsInCurrentWriter = 0;
  }
}
