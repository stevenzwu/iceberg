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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Adaptive assembly logic for the v4+ root manifest (Layer 2 of the adaptive manifest tree).
 *
 * <p>Content entries are routed into per-(content-type, lifecycle) pools and each pool
 * streaming-spills to leaf manifests only once its projected on-disk size crosses the target
 * ({@code commit.manifest.target-size-bytes}); the sub-target remainder stays as direct rows in the
 * root. A pool whose projected size never crosses the target produces no leaf at all — its entries
 * stay entirely in root (the small-write path).
 *
 * <p>This class holds the pure routing/spill decisions; the leaf-writer I/O and the {@code applyV4}
 * wiring are layered on top of it. Keeping the decision logic free of I/O makes the streaming-spill
 * partitioning directly unit-testable.
 */
class V4RootManifestAssembler {

  private V4RootManifestAssembler() {}

  /**
   * The six spill pools: one live pool and two terminal-retirement pools (DELETED, REPLACED) per
   * content type. Equality deletes are whole-file add/delete, so {@link #EQ_DELETE_REPLACED_RETIRE}
   * is structurally present but expected to stay empty (REPLACED is a data-file DV/column-update
   * concept).
   */
  enum PoolKind {
    DATA_LIVE,
    EQ_DELETE_LIVE,
    DATA_DELETED_RETIRE,
    DATA_REPLACED_RETIRE,
    EQ_DELETE_DELETED_RETIRE,
    EQ_DELETE_REPLACED_RETIRE
  }

  /**
   * Routes a direct content entry to its pool from its content type and per-snapshot root status.
   * Only {@link FileContent#DATA} and {@link FileContent#EQUALITY_DELETES} are direct entries;
   * manifest-reference rows ({@code *_MANIFEST}) are not pooled.
   */
  static PoolKind classify(FileContent contentType, EntryStatus status) {
    Preconditions.checkArgument(
        contentType == FileContent.DATA || contentType == FileContent.EQUALITY_DELETES,
        "Not a direct content entry: %s",
        contentType);
    boolean isDelete = contentType == FileContent.EQUALITY_DELETES;
    switch (status) {
      case DELETED:
        return isDelete ? PoolKind.EQ_DELETE_DELETED_RETIRE : PoolKind.DATA_DELETED_RETIRE;
      case REPLACED:
        return isDelete ? PoolKind.EQ_DELETE_REPLACED_RETIRE : PoolKind.DATA_REPLACED_RETIRE;
      case ADDED:
      case EXISTING:
      case MODIFIED:
        return isDelete ? PoolKind.EQ_DELETE_LIVE : PoolKind.DATA_LIVE;
      default:
        throw new IllegalArgumentException("Unsupported entry status: " + status);
    }
  }

  /** The result of spilling one pool: zero or more leaf batches plus the root-direct remainder. */
  static final class SpillResult<T> {
    private final List<List<T>> leafBatches;
    private final List<T> rootRemainder;

    SpillResult(List<List<T>> leafBatches, List<T> rootRemainder) {
      this.leafBatches = leafBatches;
      this.rootRemainder = rootRemainder;
    }

    /**
     * Batches that overflowed the target and are written as leaf manifests (each ~target-sized).
     */
    List<List<T>> leafBatches() {
      return leafBatches;
    }

    /** The trailing sub-target entries kept as direct rows in the root manifest. */
    List<T> rootRemainder() {
      return rootRemainder;
    }
  }

  /**
   * Partitions one pool's entries (in arrival order) into leaf batches plus a root-direct
   * remainder, mirroring the streaming spill: entries accumulate until the running projected size
   * reaches {@code targetBytes}, at which point the accumulated batch is emitted as a leaf and a
   * fresh batch starts; the trailing batch (projected {@code < targetBytes}) is the root remainder.
   * A pool whose total never reaches the target emits no leaf and keeps every entry as the
   * remainder.
   *
   * <p>Projected size uses a uniform per-entry estimate ({@code avgBytesPerEntry}, seeded from the
   * most recent prior leaf of this bucket or a schema-based fallback). The rolling writer's actual
   * {@code length()} corrects any drift at write time; this projection only decides the split.
   *
   * @param entries pool entries in arrival order
   * @param avgBytesPerEntry projected on-disk bytes per entry (must be positive)
   * @param targetBytes per-leaf target size in bytes (must be positive)
   */
  static <T> SpillResult<T> partitionForSpill(
      List<T> entries, double avgBytesPerEntry, long targetBytes) {
    Preconditions.checkArgument(avgBytesPerEntry > 0, "avgBytesPerEntry must be > 0");
    Preconditions.checkArgument(targetBytes > 0, "targetBytes must be > 0");

    List<List<T>> leafBatches = Lists.newArrayList();
    List<T> current = Lists.newArrayList();
    for (T entry : entries) {
      current.add(entry);
      if (projectedBytes(current.size(), avgBytesPerEntry) >= targetBytes) {
        leafBatches.add(current);
        current = Lists.newArrayList();
      }
    }

    // The trailing (sub-target) batch stays in root as direct rows.
    return new SpillResult<>(leafBatches, current);
  }

  private static long projectedBytes(int count, double avgBytesPerEntry) {
    return (long) (count * avgBytesPerEntry);
  }
}
