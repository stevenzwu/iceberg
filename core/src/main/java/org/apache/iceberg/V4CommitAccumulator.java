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
import org.apache.iceberg.V4RootManifestAssembler.PoolKind;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;

/**
 * Routes {@link TrackedFile} rows into per-(content-type, lifecycle) pools during a commit and,
 * at {@link #close(TrackedFileWriter.RootState)}, promotes the live-data pool's last streamed
 * writer to the snapshot's root manifest — carrying every rolled leaf's manifest-reference row
 * plus each retirement pool's sub-target tail as direct rows.
 *
 * <p>Live-data rows stream through a {@link V4StreamingWriter} that opens a fresh
 * {@link TrackedFileWriter} on the first {@link #add}, rolls it at each projection crossing, and
 * keeps the last-open writer available at close time so promotion can append leaf-refs + extras
 * into it — one fewer file per commit than a separate root writer approach.
 *
 * <p>Retirement pools (DELETED / REPLACED) and the eq-delete placeholders keep the buffered
 * semantics of {@link V4WritePool}: rows accumulate in memory until the projected size crosses
 * the target, at which point they spill through {@link RollingTrackedFileWriter}. Retirement is
 * typically small so this preserves the "no writer opened" small-write optimization for those
 * pools — their tails flow into the promoted root as direct rows, their leaves flow in as
 * leaf-manifest-entry refs.
 *
 * <p>Callers hand external leaf references (imported manifests, DV-carrying manifests,
 * parent-snapshot carry-overs) via {@link #addExternalLeafReference}; they land in the promoted
 * root alongside the accumulator's own rolled/spilled leaves.
 */
class V4CommitAccumulator {

  private final Iterable<TrackedFile> priorRootLiveLeafEntries;

  private final V4StreamingWriter dataLive;
  private final V4WritePool dataDeletedRetirement;
  private final V4WritePool dataReplacedRetirement;

  // Placeholders for delete pools (Phase 6 owns eq-delete writes; see class doc).
  private final V4WritePool deletesLive;
  private final V4WritePool deletesDeletedRetirement;

  private final List<Pair<ManifestFile, EntryStatus>> externalLeafRefs = Lists.newArrayList();

  private boolean closed = false;
  private ManifestFile promotedRoot;

  /**
   * @param dataLeafWriterFactory supplies a fresh {@link TrackedFileWriter} on each roll (live
   *     pool) and each spill (retirement pools); the same factory backs all three data pools
   *     since a data leaf is a data leaf regardless of source pool
   * @param targetBytes per-leaf target size in bytes; shared by every pool
   * @param avgBytesPerEntry projected on-disk bytes per row; shared by every pool
   * @param priorRootLiveLeafEntries leaf-manifest-entry rows carried over unchanged from the
   *     parent snapshot's root manifest; may be empty. Land as direct rows in the promoted root
   *     ahead of any retirement pool tails.
   */
  V4CommitAccumulator(
      Supplier<TrackedFileWriter> dataLeafWriterFactory,
      long targetBytes,
      double avgBytesPerEntry,
      Iterable<TrackedFile> priorRootLiveLeafEntries) {
    Preconditions.checkArgument(
        dataLeafWriterFactory != null, "Invalid data leaf writer factory: null");
    Preconditions.checkArgument(
        priorRootLiveLeafEntries != null, "Invalid prior root live leaf entries: null");

    this.priorRootLiveLeafEntries = priorRootLiveLeafEntries;
    long avgBytesSeed = Math.max(1L, (long) avgBytesPerEntry);
    this.dataLive = new V4StreamingWriter(dataLeafWriterFactory, targetBytes, avgBytesSeed);
    this.dataDeletedRetirement =
        new V4WritePool(dataLeafWriterFactory, targetBytes, avgBytesPerEntry, EntryStatus.DELETED);
    this.dataReplacedRetirement =
        new V4WritePool(dataLeafWriterFactory, targetBytes, avgBytesPerEntry, EntryStatus.REPLACED);
    // Delete pools are wired to the same data-leaf factory only to satisfy the V4WritePool
    // constructor; add() rejects EQUALITY_DELETES so the factory is never invoked for these pools.
    // Phase 6 replaces these with delete-leaf factories.
    this.deletesLive =
        new V4WritePool(dataLeafWriterFactory, targetBytes, avgBytesPerEntry, EntryStatus.ADDED);
    this.deletesDeletedRetirement =
        new V4WritePool(dataLeafWriterFactory, targetBytes, avgBytesPerEntry, EntryStatus.DELETED);
  }

  /**
   * Routes a row into the pool matching its {@link FileContent} and tracking status.
   *
   * <p>The {@code isLive} parameter is retained for API symmetry with the reader-side scan path
   * but is not consulted for pool routing; the row's own tracking status determines its pool via
   * {@link V4RootManifestAssembler#classify(FileContent, EntryStatus)}.
   *
   * @throws UnsupportedOperationException if the row is an equality delete (delete pools are
   *     Phase 6 work)
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
   * Records an external leaf manifest as a reference to be written into the promoted root at
   * close time, with the given {@link EntryStatus}. Used for imported manifests, DV-carrying
   * manifests still on the legacy write path, and parent-snapshot carry-overs — leaves that were
   * not produced by this accumulator but must appear as leaf-manifest-entries in the root.
   */
  void addExternalLeafReference(ManifestFile leafManifest, EntryStatus status) {
    Preconditions.checkState(!closed, "Cannot add to a closed V4CommitAccumulator");
    Preconditions.checkArgument(leafManifest != null, "Invalid external leaf: null");
    Preconditions.checkArgument(status != null, "Invalid status: null");
    externalLeafRefs.add(Pair.of(leafManifest, status));
  }

  /**
   * Closes every pool, gathers direct-row extras (prior-root live leaf entries + retirement pool
   * tails) and leaf-manifest-entry refs (from this accumulator's rolled/spilled leaves and the
   * caller's external references), and promotes the live pool's still-open writer to the
   * snapshot's root manifest by appending the extras + refs into it. Returns the promoted root's
   * {@link ManifestFile}; its {@code path} is the snapshot's {@code rootManifestLocation}.
   *
   * <p>Idempotent — a second call returns the same {@code promotedRoot}.
   *
   * @param refState commit metadata (snapshot-id, sequence-number, initial nextRowId) required to
   *     resolve UNASSIGNED_SEQ on external leaf refs and assign first-row-ids where absent
   */
  ManifestFile close(TrackedFileWriter.RootState refState) {
    if (closed) {
      return promotedRoot;
    }
    Preconditions.checkArgument(refState != null, "Invalid RootState: null");

    // Close retirement + eq-delete pools first; they finalize their leaves and expose tails.
    dataDeletedRetirement.close();
    dataReplacedRetirement.close();
    deletesLive.close();
    deletesDeletedRetirement.close();

    // Direct-row extras for the promoted root, in a deterministic order: prior-root live leaf
    // entries first (preserving the parent snapshot's on-disk order for unchanged references),
    // then retirement tails, then eq-delete tails.
    List<TrackedFile> extras = Lists.newArrayList();
    for (TrackedFile row : priorRootLiveLeafEntries) {
      extras.add(row);
    }
    extras.addAll(dataDeletedRetirement.rootDirectRows());
    extras.addAll(dataReplacedRetirement.rootDirectRows());
    extras.addAll(deletesLive.rootDirectRows());
    extras.addAll(deletesDeletedRetirement.rootDirectRows());

    // Leaf-manifest-entry refs: dataLive's rolled leaves (ADDED), retirement pool leaves
    // (DELETED / REPLACED), eq-delete pool leaves (ADDED / DELETED — empty in this slice), plus
    // caller-supplied external refs.
    List<Pair<ManifestFile, EntryStatus>> refs = Lists.newArrayList();
    for (ManifestFile leaf : dataLive.peekRolledLeaves()) {
      refs.add(Pair.of(leaf, EntryStatus.ADDED));
    }
    appendRefs(refs, dataDeletedRetirement);
    appendRefs(refs, dataReplacedRetirement);
    appendRefs(refs, deletesLive);
    appendRefs(refs, deletesDeletedRetirement);
    refs.addAll(externalLeafRefs);

    this.promotedRoot = dataLive.promoteCurrentToRoot(extras, refs, refState);
    this.closed = true;
    return promotedRoot;
  }

  /** Returns the promoted root {@link ManifestFile}. Requires {@link #close} to have run. */
  ManifestFile promotedRoot() {
    Preconditions.checkState(closed, "V4CommitAccumulator is not closed yet");
    return promotedRoot;
  }

  /**
   * Returns every leaf manifest produced by this commit across all pools (live streaming + all
   * retirement / eq-delete spills). Does not include the promoted root file, which is exposed
   * separately via {@link #promotedRoot}.
   */
  List<ManifestFile> leafManifests() {
    Preconditions.checkState(closed, "V4CommitAccumulator is not closed yet");
    ImmutableList.Builder<ManifestFile> builder = ImmutableList.builder();
    builder.addAll(dataLive.completedLeaves());
    builder.addAll(dataDeletedRetirement.leafManifests());
    builder.addAll(dataReplacedRetirement.leafManifests());
    builder.addAll(deletesLive.leafManifests());
    builder.addAll(deletesDeletedRetirement.leafManifests());
    return builder.build();
  }

  private static void appendRefs(List<Pair<ManifestFile, EntryStatus>> out, V4WritePool pool) {
    EntryStatus status = pool.leafRowStatus();
    for (ManifestFile leaf : pool.leafManifests()) {
      out.add(Pair.of(leaf, status));
    }
  }
}
