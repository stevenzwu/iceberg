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
import org.apache.iceberg.V4RootManifestAssembler.PoolKind;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;

/**
 * Routes {@link TrackedFile} rows into per-(content-type, lifecycle) pools during a commit and, at
 * {@link #close(long, long, Long)}, promotes the live-data pool's last streamed writer to the
 * snapshot's root manifest — folding each retirement pool's sub-target tail in as direct rows and
 * every rolled/spilled leaf in as a leaf-manifest-entry.
 *
 * <p>Live-data rows stream through a {@link StreamingLeafManifestWriter} that opens a fresh {@link
 * LeafManifestWriter} on the first {@link #add}, rolls it at each projection crossing, and hands
 * its last-open writer to {@link #close} as an open {@link RootManifestWriter} — one fewer file per
 * commit than a separate root writer approach. Parent-snapshot carry-over live rows are fed through
 * {@link #add} like any other live row, so they roll and inline uniformly.
 *
 * <p>Retirement rows (DELETED / REPLACED) buffer through a {@link BufferedLeafManifestWriter}: rows
 * accumulate in memory until the projected size crosses the target, then spill as leaves.
 * Retirement is typically small, so this preserves the "no writer opened" small-write optimization
 * for those pools — their sub-target tails flow into the promoted root as direct rows, their
 * spilled leaves as leaf-manifest-entry refs. Equality-delete pools are Phase 6 work; {@link #add}
 * rejects them today.
 *
 * <p>Callers hand external leaf references (imported manifests, DV-carrying manifests,
 * parent-snapshot carry-overs) via {@link #addExternalLeafReference}; they land in the promoted
 * root alongside the accumulator's own rolled/spilled leaves.
 */
class V4CommitAccumulator {

  private final StreamingLeafManifestWriter dataLive;
  private final BufferedLeafManifestWriter dataDeletedRetirement;
  private final BufferedLeafManifestWriter dataReplacedRetirement;

  private final List<Pair<ManifestFile, EntryStatus>> externalLeafRefs = Lists.newArrayList();

  private boolean closed = false;
  private ManifestListFile promotedRoot;

  /**
   * @param dataLeafWriterFactory supplies a fresh {@link LeafManifestWriter} on each roll (live
   *     pool) and each spill (retirement pools); the same factory backs all data pools since a data
   *     leaf is a data leaf regardless of source pool
   * @param targetBytes per-leaf target size in bytes; shared by every pool
   * @param avgBytesPerEntry projected on-disk bytes per row; shared by every pool
   */
  V4CommitAccumulator(
      Supplier<LeafManifestWriter> dataLeafWriterFactory,
      long targetBytes,
      double avgBytesPerEntry) {
    Preconditions.checkArgument(
        dataLeafWriterFactory != null, "Invalid data leaf writer factory: null");

    long avgBytesSeed = Math.max(1L, (long) avgBytesPerEntry);
    this.dataLive =
        new StreamingLeafManifestWriter(dataLeafWriterFactory, targetBytes, avgBytesSeed);
    this.dataDeletedRetirement =
        new BufferedLeafManifestWriter(dataLeafWriterFactory, targetBytes, avgBytesSeed);
    this.dataReplacedRetirement =
        new BufferedLeafManifestWriter(dataLeafWriterFactory, targetBytes, avgBytesSeed);
  }

  /**
   * Routes a row into the pool matching its {@link FileContent} and tracking status.
   *
   * <p>The {@code isLive} parameter is retained for API symmetry with the reader-side scan path but
   * is not consulted for pool routing; the row's own tracking status determines its pool via {@link
   * V4RootManifestAssembler#classify(FileContent, EntryStatus)}.
   *
   * @throws UnsupportedOperationException if the row is an equality delete (delete pools are Phase
   *     6 work)
   */
  void add(TrackedFile row, boolean isLive) {
    Preconditions.checkState(!closed, "Cannot add to a closed V4CommitAccumulator");
    Preconditions.checkNotNull(row, "TrackedFile row cannot be null");
    EntryStatus status = row.tracking().status();
    PoolKind kind = V4RootManifestAssembler.classify(row.contentType(), status);
    switch (kind) {
      case DATA_LIVE:
        dataLive.add(row);
        break;
      case DATA_DELETED_RETIRE:
        dataDeletedRetirement.add(row);
        break;
      case DATA_REPLACED_RETIRE:
        dataReplacedRetirement.add(row);
        break;
      case EQ_DELETE_LIVE:
      case EQ_DELETE_DELETED_RETIRE:
      case EQ_DELETE_REPLACED_RETIRE:
        throw new UnsupportedOperationException(
            "Delete pools are not implemented; Phase 6 owns eq-delete writes");
      default:
        throw new IllegalStateException("Unknown pool kind: " + kind);
    }
  }

  /**
   * Records an external leaf manifest as a reference to be written into the promoted root at close
   * time, with the given {@link EntryStatus}. Used for imported manifests, DV-carrying manifests
   * still on the legacy write path, and parent-snapshot carry-overs — leaves that were not produced
   * by this accumulator but must appear as leaf-manifest-entries in the root.
   */
  void addExternalLeafReference(ManifestFile leafManifest, EntryStatus status) {
    Preconditions.checkState(!closed, "Cannot add to a closed V4CommitAccumulator");
    Preconditions.checkArgument(leafManifest != null, "Invalid external leaf: null");
    Preconditions.checkArgument(status != null, "Invalid status: null");
    externalLeafRefs.add(Pair.of(leafManifest, status));
  }

  /**
   * Closes the retirement pools and promotes the live pool's still-open writer to the snapshot's
   * root manifest, then drives the remaining content into the returned open {@link
   * RootManifestWriter}: each retirement pool's sub-target tail as direct rows, and every
   * rolled/spilled leaf plus the caller's external references as leaf-manifest-entries. Returns the
   * root's {@link ManifestListFile}; its {@code location} is the snapshot's {@code
   * rootManifestLocation}.
   *
   * <p>Idempotent — a second call returns the same {@code promotedRoot}.
   *
   * @param snapshotId the committing snapshot id, used to resolve UNASSIGNED_SEQ on refs
   * @param sequenceNumber the committing sequence number
   * @param nextRowId the initial first-row-id counter for freshly-written DATA manifest refs
   */
  ManifestListFile close(long snapshotId, long sequenceNumber, Long nextRowId) {
    if (closed) {
      return promotedRoot;
    }

    // Retirement pools finalize first: take each sub-target tail (direct rows) so the promoted root
    // can inline them; their spilled leaves are referenced below.
    List<TrackedFile> deletedTail = dataDeletedRetirement.closeAndTakeTail();
    List<TrackedFile> replacedTail = dataReplacedRetirement.closeAndTakeTail();

    // Promote the live pool's still-open writer; this method drives everything else into the
    // returned open root writer and owns its close().
    RootManifestWriter root = dataLive.promoteCurrentToRoot(snapshotId, sequenceNumber, nextRowId);

    // Direct rows: retirement pool tails.
    deletedTail.forEach(root::add);
    replacedTail.forEach(root::add);

    // Leaf-manifest-entries: dataLive's rolled leaves (ADDED), retirement pool leaves (DELETED /
    // REPLACED), then caller-supplied external refs.
    for (ManifestFile leaf : dataLive.completedLeaves()) {
      root.addManifestEntry(leaf, EntryStatus.ADDED);
    }

    for (ManifestFile leaf : dataDeletedRetirement.toManifestFiles()) {
      root.addManifestEntry(leaf, EntryStatus.DELETED);
    }

    for (ManifestFile leaf : dataReplacedRetirement.toManifestFiles()) {
      root.addManifestEntry(leaf, EntryStatus.REPLACED);
    }

    for (Pair<ManifestFile, EntryStatus> ref : externalLeafRefs) {
      root.addManifestEntry(ref.first(), ref.second());
    }

    try {
      root.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close promoted root manifest writer", e);
    }

    this.promotedRoot = root.toRootManifestFile();
    this.closed = true;
    return promotedRoot;
  }

  /** Returns the promoted root {@link ManifestListFile}. Requires {@link #close} to have run. */
  ManifestListFile promotedRoot() {
    Preconditions.checkState(closed, "V4CommitAccumulator is not closed yet");
    return promotedRoot;
  }

  /**
   * Returns every leaf manifest produced by this commit across all pools (live streaming + all
   * retirement spills). Does not include the promoted root file, which is exposed separately via
   * {@link #promotedRoot}.
   */
  List<ManifestFile> leafManifests() {
    Preconditions.checkState(closed, "V4CommitAccumulator is not closed yet");
    ImmutableList.Builder<ManifestFile> builder = ImmutableList.builder();
    builder.addAll(dataLive.completedLeaves());
    builder.addAll(dataDeletedRetirement.toManifestFiles());
    builder.addAll(dataReplacedRetirement.toManifestFiles());
    return builder.build();
  }
}
