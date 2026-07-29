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

/**
 * Marker {@link GenericManifestFile} representing the DATA direct rows carried inline in a v4+
 * promoted root manifest. Surfaced by {@link RootManifestReader#read} alongside the real
 * leaf-manifest-entries so consumers of {@link Snapshot#dataManifests} (notably scan planning via
 * {@link ManifestGroup}) see the inline data files in root.
 *
 * <p>The {@link #path()} of a virtual manifest is the root manifest's own location — {@link
 * ManifestFiles#read} detects this marker type and opens a {@link V4ManifestReader} that filters
 * rows to {@link FileContent#DATA}, skipping the co-resident {@code DATA_MANIFEST} / {@code
 * DELETE_MANIFEST} rows.
 *
 * <p>The file-level partition summary is left null: the underlying file is a root manifest, not a
 * leaf, and the direct rows are written under the union partition type. Consumers requiring
 * per-file partition summaries should read the entries themselves.
 *
 * <p>The direct rows are already materialized by {@link RootManifestReader#read} while classifying
 * root-manifest rows into leaf-manifest-entries vs. inline DATA rows in root. Caching them here
 * lets {@link ManifestFiles#read} skip a second parquet decode of the same root manifest on the
 * scan path.
 */
class RootDirectRowsManifestFile extends GenericManifestFile {
  private final List<TrackedFile> cachedDirectRows;

  RootDirectRowsManifestFile(
      String rootPath,
      long rootLength,
      int partitionSpecId,
      long sequenceNumber,
      long minSequenceNumber,
      Long snapshotId,
      int addedFilesCount,
      long addedRowsCount,
      int existingFilesCount,
      long existingRowsCount,
      int deletedFilesCount,
      long deletedRowsCount,
      Long firstRowId,
      int formatVersion,
      Integer replacedFilesCount,
      Long replacedRowsCount,
      List<TrackedFile> cachedDirectRows) {
    super(
        rootPath,
        rootLength,
        partitionSpecId,
        ManifestContent.DATA,
        sequenceNumber,
        minSequenceNumber,
        snapshotId,
        null /* partition summaries not synthesized for virtual manifests */,
        null /* keyMetadata */,
        addedFilesCount,
        addedRowsCount,
        existingFilesCount,
        existingRowsCount,
        deletedFilesCount,
        deletedRowsCount,
        firstRowId,
        null /* recordCount unavailable for a virtual view */,
        formatVersion,
        replacedFilesCount,
        replacedRowsCount);
    this.cachedDirectRows = cachedDirectRows;
  }

  /**
   * Returns the direct DATA rows already decoded from the root manifest during {@link
   * RootManifestReader#read}. {@link ManifestFiles#read} feeds these into {@link
   * V4ManifestReader#forCachedRows} to avoid re-opening the root file for the scan pass.
   */
  List<TrackedFile> cachedDirectRows() {
    return cachedDirectRows;
  }
}
