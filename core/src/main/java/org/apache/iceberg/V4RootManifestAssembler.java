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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Adaptive assembly logic for the v4+ root manifest (Layer 2 of the adaptive manifest tree).
 *
 * <p>Content entries are routed into per-(content-type, lifecycle) pools and each pool
 * streaming-spills to leaf manifests only once its projected on-disk size crosses the target
 * ({@code commit.manifest.target-size-bytes}); the sub-target remainder stays as direct rows in the
 * root. A pool whose projected size never crosses the target produces no leaf at all — its entries
 * stay entirely in root (the small-write path).
 *
 * <p>This class holds the pure routing/spill decisions; the leaf-writer I/O and the {@code
 * applyRootManifest} wiring are layered on top of it. Keeping the decision logic free of I/O makes
 * the streaming-spill partitioning directly unit-testable.
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
   * leaf-manifest-entry rows ({@code *_MANIFEST}) are not pooled.
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
}
