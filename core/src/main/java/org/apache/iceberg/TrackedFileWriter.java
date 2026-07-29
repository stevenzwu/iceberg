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
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * A pure {@link FileAppender} for v4+ {@code content_entry} rows — the on-disk format shared by
 * root manifests (the v4+ replacement for the manifest list) and leaf data/delete manifests. It
 * writes fully-formed {@link TrackedFile} rows verbatim and does no role-specific bookkeeping.
 *
 * <p>Role wrappers layer on top: {@link LeafManifestWriter} adds per-status counters and builds a
 * leaf {@link ManifestFile}; {@link RootManifestWriter} adds manifest-reference resolution and
 * builds a {@link SnapshotFile}. Both construct the appropriate {@code content_entry} appender
 * (leaf vs root schema and metadata) and wrap it in an instance of this class.
 *
 * <p>The static schema helpers here ({@link #ROOT_CONTENT_STATS_TYPE}, {@link
 * #EMPTY_PARTITION_PLACEHOLDER}) are the shared schema contract, consumed by the writers above and
 * the readers ({@code RootManifestReader}, {@code V4ManifestReader}).
 */
class TrackedFileWriter implements FileAppender<TrackedFile> {
  /**
   * Content stats type for the root manifest. Root manifest entries do not carry column-level
   * stats, so a placeholder struct with a single dummy optional boolean field is used. Parquet
   * cannot encode an empty struct, so this placeholder is always written as null and ignored on
   * read.
   */
  static final Types.StructType ROOT_CONTENT_STATS_TYPE =
      Types.StructType.of(Types.NestedField.optional(99998, "_no_stats", Types.BooleanType.get()));

  /**
   * Single-field placeholder partition struct used as the partition <em>read</em> projection by
   * readers that may lack the real partition spec (see {@link #emptyPartitionPlaceholderIfNeeded}).
   * Field-id projection through this struct reads any physical partition column (partitioned tables
   * have one on their direct rows) as null; unpartitioned manifests carry no partition column and
   * also read null. It is never written to disk: writers use the table's real partition type, and
   * unpartitioned tables store no partition column (the empty type maps to {@link
   * org.apache.iceberg.types.Types.UnknownType} and is omitted).
   */
  static final Types.StructType EMPTY_PARTITION_PLACEHOLDER =
      Types.StructType.of(
          Types.NestedField.optional(99999, "_unpartitioned", Types.BooleanType.get()));

  /**
   * Returns the partition type to project when <em>reading</em>: the input if it has fields, or
   * {@link #EMPTY_PARTITION_PLACEHOLDER} when empty. Readers that may lack the real partition spec
   * use this so a physical partition column projects to null via field-id mismatch, and an absent
   * column (unpartitioned) also reads null. Writers do not use it: they pass the table's real
   * partition type, so unpartitioned tables store no partition column.
   */
  static Types.StructType emptyPartitionPlaceholderIfNeeded(Types.StructType partitionType) {
    return partitionType.fields().isEmpty() ? EMPTY_PARTITION_PLACEHOLDER : partitionType;
  }

  private final OutputFile outputFile;
  private final FileAppender<StructLike> appender;
  private boolean closed = false;

  TrackedFileWriter(OutputFile outputFile, FileAppender<StructLike> appender) {
    this.outputFile = outputFile;
    this.appender = appender;
  }

  /** Returns the file this writer appends to. */
  OutputFile outputFile() {
    return outputFile;
  }

  /**
   * Appends a {@link TrackedFile} row verbatim. All TrackedFile implementations in this package
   * also implement {@link StructLike}; the row is cast and handed to the underlying Parquet
   * appender directly.
   */
  @Override
  public void add(TrackedFile row) {
    Preconditions.checkNotNull(row, "TrackedFile row cannot be null");
    appender.add((StructLike) row);
  }

  @Override
  public Metrics metrics() {
    return appender.metrics();
  }

  @Override
  public long length() {
    return appender.length();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      this.closed = true;
      appender.close();
    }
  }
}
