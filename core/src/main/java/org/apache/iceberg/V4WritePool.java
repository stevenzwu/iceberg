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
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Per-pool wrapper around a {@link RollingTrackedFileWriter} used by {@link V4CommitAccumulator} to
 * implement the adaptive small-write optimization: rows accumulate in the rolling writer's in-memory
 * buffer while the projected on-disk size stays below the per-leaf target; once the projection
 * crosses the target on {@link #add(TrackedFile)}, the buffered rows drain as leaf-sized batches
 * through the rolling writer.
 *
 * <p>Two shapes appear after {@link #close()}:
 *
 * <ul>
 *   <li>Never spilled — the rolling writer's buffer is returned intact from
 *       {@link RollingTrackedFileWriter#closeAndTakeTail()} as {@link #rootDirectRows()} and no
 *       leaf manifest was produced. This is the small-write path where a whole pool fits under one
 *       leaf target and stays inline in the root manifest.
 *   <li>Spilled at least once — {@link #leafManifests()} carries the ~target-sized batches written
 *       to disk and {@link #rootDirectRows()} carries the sub-target tail returned by
 *       {@link RollingTrackedFileWriter#closeAndTakeTail()} (may be empty).
 * </ul>
 *
 * <p>{@link #leafRowStatus()} echoes the constructor argument so the accumulator can stamp the
 * matching {@link EntryStatus} on each {@link ManifestFile} reference row emitted for a spilled
 * leaf.
 */
class V4WritePool {

  private final EntryStatus leafRowStatus;
  private final RollingTrackedFileWriter rollingWriter;

  private boolean closed = false;
  private List<TrackedFile> rootDirectRows = ImmutableList.of();
  private List<ManifestFile> leafManifests = ImmutableList.of();
  private boolean spilled = false;

  /**
   * @param leafWriterFactory supplies a fresh {@link TrackedFileWriter} for each spilled leaf; not
   *     invoked while the pool stays under the target
   * @param targetBytes per-leaf target size in bytes; must be positive
   * @param avgBytesPerEntry projected on-disk bytes per row used to seed the buffer's size
   *     projection; must be positive
   * @param leafRowStatus {@link EntryStatus} to stamp on leaf-manifest-entry rows the accumulator
   *     writes for leaves produced by this pool (ADDED for live pools, DELETED/REPLACED for
   *     retirement pools)
   */
  V4WritePool(
      Supplier<TrackedFileWriter> leafWriterFactory,
      long targetBytes,
      double avgBytesPerEntry,
      EntryStatus leafRowStatus) {
    Preconditions.checkArgument(leafWriterFactory != null, "Invalid leaf writer factory: null");
    Preconditions.checkArgument(targetBytes > 0, "targetBytes must be positive: %s", targetBytes);
    Preconditions.checkArgument(
        avgBytesPerEntry > 0, "avgBytesPerEntry must be positive: %s", avgBytesPerEntry);
    Preconditions.checkArgument(leafRowStatus != null, "Invalid leaf row status: null");
    this.leafRowStatus = leafRowStatus;
    // avgBytesPerEntry is a double for arithmetic precision but RollingTrackedFileWriter takes a
    // long seed; the constructor floors to at least 1 so the projection stays positive.
    long seed = Math.max(1L, (long) avgBytesPerEntry);
    this.rollingWriter = new RollingTrackedFileWriter(leafWriterFactory, targetBytes, seed);
  }

  /**
   * Adds a row to the pool via the underlying {@link RollingTrackedFileWriter}, which buffers the
   * row and, if its projected size crosses the target on this add, flushes the current batch as a
   * leaf manifest.
   */
  void add(TrackedFile row) {
    Preconditions.checkState(!closed, "Cannot add to a closed V4WritePool");
    Preconditions.checkNotNull(row, "TrackedFile row cannot be null");
    rollingWriter.add(row);
  }

  /**
   * Captures the pool's outputs by taking the rolling writer's sub-target tail as
   * {@link #rootDirectRows()} and the flushed batches as {@link #leafManifests()}. Idempotent.
   *
   * <p>Never-spilled pools return the entire buffered content as the tail (no leaf written).
   * Spilled pools return whatever rows were buffered after the last flush.
   */
  void close() {
    if (closed) {
      return;
    }
    List<TrackedFile> tail = rollingWriter.closeAndTakeTail();
    List<ManifestFile> leaves = rollingWriter.toManifestFiles();
    this.rootDirectRows = ImmutableList.copyOf(tail);
    this.leafManifests = ImmutableList.copyOf(leaves);
    this.spilled = !leaves.isEmpty();
    this.closed = true;
  }

  /** Returns whether the pool ever crossed the target and produced (or attempted) a leaf. */
  boolean spilled() {
    Preconditions.checkState(closed, "V4WritePool is not closed yet");
    return spilled;
  }

  /**
   * Returns the rows destined for direct entries in the root manifest: either the full buffer (if
   * never spilled) or the trailing sub-target tail from the rolling writer (if spilled).
   */
  List<TrackedFile> rootDirectRows() {
    Preconditions.checkState(closed, "V4WritePool is not closed yet");
    return rootDirectRows;
  }

  /** Returns the leaf {@link ManifestFile}s produced by spilled batches (empty if never spilled). */
  List<ManifestFile> leafManifests() {
    Preconditions.checkState(closed, "V4WritePool is not closed yet");
    return leafManifests;
  }

  /**
   * Returns the {@link EntryStatus} to stamp on leaf-manifest-entry rows for this pool's leaves.
   * Echoes the constructor argument so the accumulator does not need to remember which pool a leaf
   * came from.
   */
  EntryStatus leafRowStatus() {
    return leafRowStatus;
  }
}
