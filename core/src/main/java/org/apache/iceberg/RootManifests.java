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
import java.util.Map;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * Factory for v4+ root manifest readers and writers. Root manifests are the v4+ replacement for the
 * manifest list; they use the {@code content_entry} Parquet schema and reference leaf data/delete
 * manifests via {@code DATA_MANIFEST} / {@code DELETE_MANIFEST} entries.
 *
 * <p>Analogous to {@link ManifestLists} for v1–v3.
 */
class RootManifests {
  private RootManifests() {}

  /**
   * Reads a v4+ root manifest and returns the list of {@link ManifestFile} objects.
   *
   * <p>Leaf-manifest-entry rows leave the partition column null on write; direct data-file entries
   * in root (small-write optimization) are skipped by {@link RootManifestReader} and so do not need
   * accurate partition decoding here.
   */
  static List<ManifestFile> read(InputFile rootManifest) {
    return RootManifestReader.read(rootManifest);
  }

  /**
   * Creates a new {@link RootManifestWriter} for a v4+ root manifest with the null-stats
   * placeholder content_stats type — appropriate for a root that carries only manifest reference
   * rows. Callers that intend to append direct content-file rows (Phase 3 adaptive assembler) use
   * the overload that accepts an explicit {@code contentStatsType}.
   *
   * @param formatVersion the table format version; must be {@code >= 4}
   * @param outputFile the output file to write to
   * @param encryptionManager the encryption manager for the table
   * @param snapshotId the snapshot ID being committed
   * @param parentSnapshotId the parent snapshot ID, or null for the first snapshot
   * @param sequenceNumber the sequence number for the new snapshot
   * @param firstRowId the snapshot's first-row-id (initializes the per-data-manifest counter that
   *     assigns first-row-id values to DATA manifest references that lack one)
   * @param tableSchema the table's current schema; used together with {@code specsById} to compute
   *     the union partition type written for every content_entry row
   * @param specsById the table's full live partition spec map; the writer derives the partition
   *     column shape from {@link Partitioning#partitionType(Schema, java.util.Collection)} so the
   *     root manifest partition schema matches the leaf manifests
   * @throws IllegalArgumentException if {@code formatVersion < 4}
   */
  static RootManifestWriter write(
      int formatVersion,
      OutputFile outputFile,
      EncryptionManager encryptionManager,
      long snapshotId,
      Long parentSnapshotId,
      long sequenceNumber,
      Long firstRowId,
      Schema tableSchema,
      Map<Integer, PartitionSpec> specsById) {
    return write(
        formatVersion,
        outputFile,
        encryptionManager,
        snapshotId,
        parentSnapshotId,
        sequenceNumber,
        firstRowId,
        tableSchema,
        specsById,
        TrackedFileWriter.ROOT_CONTENT_STATS_TYPE);
  }

  /**
   * Creates a new {@link RootManifestWriter} that writes stats-bearing content_entry rows on a v4+
   * root manifest. Behaves like the placeholder-stats variant but encodes the given {@code
   * content_stats} type, so direct data-file rows appended via {@link RootManifestWriter#add} carry
   * real column statistics.
   *
   * @param contentStatsType the content_stats struct type the writer encodes; must match the stats
   *     struct on every row passed to {@link RootManifestWriter#add}
   */
  static RootManifestWriter write(
      int formatVersion,
      OutputFile outputFile,
      EncryptionManager encryptionManager,
      long snapshotId,
      Long parentSnapshotId,
      long sequenceNumber,
      Long firstRowId,
      Schema tableSchema,
      Map<Integer, PartitionSpec> specsById,
      Types.StructType contentStatsType) {
    Preconditions.checkArgument(
        formatVersion >= 4,
        "Cannot write root manifest for format version %s (minimum: 4)",
        formatVersion);
    Preconditions.checkArgument(tableSchema != null, "Invalid table schema: null");
    Preconditions.checkArgument(
        specsById != null && !specsById.isEmpty(), "Invalid specs map: null or empty");
    Types.StructType partitionType = Partitioning.unionPartitionTypes(specsById.values());
    return RootManifestWriter.create(
        outputFile,
        encryptionManager,
        snapshotId,
        parentSnapshotId,
        sequenceNumber,
        firstRowId,
        partitionType,
        contentStatsType);
  }
}
