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

/**
 * A buffering roller over {@link TrackedFile} rows for pools that do not own the root manifest.
 * Parallel to {@link RollingManifestWriter} but bounded on {@link TrackedFile} rather than {@code
 * ContentFile<F>}, and — unlike {@link StreamingLeafManifestWriter} — it holds rows in memory so
 * its sub-target tail can be handed back to the caller rather than promoted to root.
 *
 * <p>Because rows are held in memory, {@link #add} retains a stats-free materialized copy rather
 * than the caller's (reusable) row. This writer buffers only retired (DELETED/REPLACED) entries,
 * which are non-live and never read for column bounds, so dropping stats is safe.
 *
 * <p>Buffers rows using a caller-provided {@code avgBytesPerEntry} seed. When the estimated
 * buffered bytes cross {@code targetSizeBytes}, the buffered rows are flushed through a fresh
 * {@link LeafManifestWriter} and closed as a leaf {@link ManifestFile}. Rows added after a flush
 * accumulate in a new buffer.
 *
 * <p>Two close modes:
 *
 * <ul>
 *   <li>{@link #close()} flushes any remaining buffer as a final leaf (may be smaller than {@code
 *       targetSizeBytes}).
 *   <li>{@link #closeAndTakeTail()} closes without flushing the remaining buffer, returning the
 *       tail rows to the caller so a sub-target tail becomes direct rows in the root manifest
 *       rather than a small leaf (the adaptive small-write optimization for retirement pools).
 * </ul>
 */
class BufferedLeafManifestWriter implements Closeable {

  private final Supplier<LeafManifestWriter> writerSupplier;
  private final long targetSizeBytes;
  private final long avgBytesPerEntry;
  private final List<TrackedFile> buffer;
  private final List<ManifestFile> manifestFiles;
  private boolean closed = false;
  private boolean tailTaken = false;

  /**
   * @param writerSupplier supplies a fresh {@link LeafManifestWriter} for each spilled leaf
   * @param targetSizeBytes byte threshold above which the buffer flushes as a leaf
   * @param avgBytesPerEntry seed used to estimate buffered bytes ({@code count *
   *     avgBytesPerEntry}); must be positive
   */
  BufferedLeafManifestWriter(
      Supplier<LeafManifestWriter> writerSupplier, long targetSizeBytes, long avgBytesPerEntry) {
    Preconditions.checkArgument(
        targetSizeBytes > 0, "targetSizeBytes must be positive: %s", targetSizeBytes);
    Preconditions.checkArgument(
        avgBytesPerEntry > 0, "avgBytesPerEntry must be positive: %s", avgBytesPerEntry);
    this.writerSupplier = writerSupplier;
    this.targetSizeBytes = targetSizeBytes;
    this.avgBytesPerEntry = avgBytesPerEntry;
    this.buffer = Lists.newArrayList();
    this.manifestFiles = Lists.newArrayList();
  }

  /**
   * Adds a row to the buffer. If the estimated buffered bytes cross {@code targetSizeBytes} after
   * the add, the buffer is flushed as a leaf.
   */
  void add(TrackedFile row) {
    Preconditions.checkState(!closed, "Cannot add to a closed BufferedLeafManifestWriter");
    // Retain an independent copy, not the caller's row: write-direction wrappers are reusable
    // (re-pointed per row), so buffering the wrapper itself would alias every buffered row to the
    // last add. Stats are dropped because this writer only buffers retired (DELETED/REPLACED)
    // entries, which are non-live and never read for column bounds.
    buffer.add(row.copyWithoutStats());
    if (estimatedBufferedBytes() >= targetSizeBytes) {
      flushBufferAsLeaf();
    }
  }

  /** Returns the current estimated buffered bytes (visible for tests / accumulator diagnostics). */
  long estimatedBufferedBytes() {
    return (long) buffer.size() * avgBytesPerEntry;
  }

  /** Returns the number of rows currently in the buffer (not yet flushed to a leaf). */
  int bufferedRowCount() {
    return buffer.size();
  }

  private void flushBufferAsLeaf() {
    if (buffer.isEmpty()) {
      return;
    }

    LeafManifestWriter writer = writerSupplier.get();
    boolean threw = true;
    try {
      for (TrackedFile row : buffer) {
        writer.add(row);
      }

      writer.close();
      manifestFiles.add(writer.toManifestFile());
      buffer.clear();
      threw = false;
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to flush BufferedLeafManifestWriter buffer to leaf", e);
    } finally {
      if (threw) {
        try {
          writer.close();
        } catch (Exception suppressed) {
          // best-effort cleanup — swallow to avoid masking the original failure
        }
      }
    }
  }

  /**
   * Closes the rolling writer, flushing any remaining buffered rows as a final (possibly
   * sub-target) leaf. Mutually exclusive with {@link #closeAndTakeTail()}.
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    flushBufferAsLeaf();
    this.closed = true;
  }

  /**
   * Closes without flushing the remaining buffer, returning the tail rows so the caller can route
   * them elsewhere (e.g., direct rows in the root manifest). Must be called before {@link
   * #close()}; the two paths are mutually exclusive.
   */
  List<TrackedFile> closeAndTakeTail() {
    Preconditions.checkState(!closed, "BufferedLeafManifestWriter already closed");
    Preconditions.checkState(!tailTaken, "Tail already taken");
    List<TrackedFile> tail = ImmutableList.copyOf(buffer);
    buffer.clear();
    tailTaken = true;
    closed = true;
    return tail;
  }

  /** Returns the closed leaf {@link ManifestFile}s produced by flushes so far. */
  List<ManifestFile> toManifestFiles() {
    Preconditions.checkState(closed, "Cannot get ManifestFile list from an unclosed writer");
    return ImmutableList.copyOf(manifestFiles);
  }
}
