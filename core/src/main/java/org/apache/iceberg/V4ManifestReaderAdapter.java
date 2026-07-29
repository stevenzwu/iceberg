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

import java.util.Map;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.util.PartitionSet;

/**
 * Adapts a {@link V4ManifestEntryProjector} to the {@link ManifestReader} API so callers can use
 * v4 content_entry manifests without code changes. Only {@link #entries()} and {@link
 * #liveEntries()} are overridden; all other methods are inherited from {@link ManifestReader}.
 *
 * @param <F> either {@link DataFile} or {@link DeleteFile}
 */
class V4ManifestReaderAdapter<F extends ContentFile<F>> extends ManifestReader<F> {
  private final V4ManifestEntryProjector projector;
  private final ManifestContent manifestContent;
  private final String manifestLocation;
  private final Long firstRowId;
  private final boolean isCommitted;
  private final boolean directRowsOnly;
  private final PartitionSpec adapterSpec;
  // Scan-config state: mirrored from the base ManifestReader's setters (which store into private
  // fields) so rawEntries() can apply partition/metrics filters and skipped-file counters — the
  // base's entries() would otherwise never run because the adapter routes entries through v4Reader.
  private Expression adapterPartFilter = Expressions.alwaysTrue();
  private Expression adapterRowFilter = Expressions.alwaysTrue();
  private PartitionSet adapterPartitionSet = null;
  private boolean adapterCaseSensitive = true;
  private ScanMetrics adapterScanMetrics = ScanMetrics.noop();
  private Evaluator lazyEvaluator = null;
  private InclusiveMetricsEvaluator lazyMetricsEvaluator = null;

  V4ManifestReaderAdapter(
      InputFile file,
      int specId,
      Map<Integer, PartitionSpec> specsById,
      V4ManifestEntryProjector projector,
      ManifestContent manifestContent) {
    this(file, specId, specsById, projector, manifestContent, null, true, false);
  }

  V4ManifestReaderAdapter(
      InputFile file,
      int specId,
      Map<Integer, PartitionSpec> specsById,
      V4ManifestEntryProjector projector,
      ManifestContent manifestContent,
      Long firstRowId,
      boolean isCommitted) {
    this(file, specId, specsById, projector, manifestContent, firstRowId, isCommitted, false);
  }

  /**
   * @param directRowsOnly when true, entries are drawn from {@link
   *     V4ManifestEntryProjector#directDataEntriesFromRoot()} — filters co-resident
   *     leaf-manifest-entry rows out of a promoted root's row iterator. Only valid for {@link
   *     ManifestContent#DATA}.
   */
  V4ManifestReaderAdapter(
      InputFile file,
      int specId,
      Map<Integer, PartitionSpec> specsById,
      V4ManifestEntryProjector projector,
      ManifestContent manifestContent,
      Long firstRowId,
      boolean isCommitted,
      boolean directRowsOnly) {
    super(
        file,
        specId,
        specsById,
        InheritableMetadataFactory.empty(),
        null /* firstRowId handled in iterator() */,
        manifestContent == ManifestContent.DATA ? FileType.DATA_FILES : FileType.DELETE_FILES);
    this.projector = projector;
    this.manifestContent = manifestContent;
    this.manifestLocation = file.location();
    this.firstRowId = firstRowId;
    this.isCommitted = isCommitted;
    this.directRowsOnly = directRowsOnly;
    PartitionSpec resolvedSpec = specsById != null ? specsById.get(specId) : null;
    this.adapterSpec = resolvedSpec != null ? resolvedSpec : PartitionSpec.unpartitioned();
    addCloseable(projector);
  }

  @Override
  public ManifestReader<F> filterPartitions(Expression expr) {
    this.adapterPartFilter = Expressions.and(adapterPartFilter, expr);
    this.lazyEvaluator = null;
    return super.filterPartitions(expr);
  }

  @Override
  public ManifestReader<F> filterPartitions(PartitionSet partitions) {
    this.adapterPartitionSet = partitions;
    return super.filterPartitions(partitions);
  }

  @Override
  public ManifestReader<F> filterRows(Expression expr) {
    this.adapterRowFilter = Expressions.and(adapterRowFilter, expr);
    this.lazyEvaluator = null;
    this.lazyMetricsEvaluator = null;
    return super.filterRows(expr);
  }

  @Override
  public ManifestReader<F> caseSensitive(boolean isCaseSensitive) {
    this.adapterCaseSensitive = isCaseSensitive;
    this.lazyEvaluator = null;
    this.lazyMetricsEvaluator = null;
    return super.caseSensitive(isCaseSensitive);
  }

  @Override
  ManifestReader<F> scanMetrics(ScanMetrics newScanMetrics) {
    this.adapterScanMetrics = newScanMetrics != null ? newScanMetrics : ScanMetrics.noop();
    return super.scanMetrics(newScanMetrics);
  }

  private Evaluator evaluator() {
    if (lazyEvaluator == null) {
      Expression projected =
          Projections.inclusive(adapterSpec, adapterCaseSensitive).project(adapterRowFilter);
      Expression finalPartFilter = Expressions.and(projected, adapterPartFilter);
      this.lazyEvaluator =
          new Evaluator(adapterSpec.partitionType(), finalPartFilter, adapterCaseSensitive);
    }

    return lazyEvaluator;
  }

  private InclusiveMetricsEvaluator metricsEvaluator() {
    if (lazyMetricsEvaluator == null) {
      this.lazyMetricsEvaluator =
          new InclusiveMetricsEvaluator(
              adapterSpec.schema(), adapterRowFilter, adapterCaseSensitive);
    }

    return lazyMetricsEvaluator;
  }

  private boolean inPartitionSet(F fileToCheck) {
    return adapterPartitionSet == null
        || adapterPartitionSet.contains(fileToCheck.specId(), fileToCheck.partition());
  }

  private boolean hasScanFilter() {
    return adapterPartFilter != Expressions.alwaysTrue()
        || adapterRowFilter != Expressions.alwaysTrue()
        || adapterPartitionSet != null;
  }

  @Override
  CloseableIterable<ManifestEntry<F>> entries() {
    return applyScanFilters(rawEntries());
  }

  @Override
  CloseableIterable<ManifestEntry<F>> liveEntries() {
    return CloseableIterable.filter(
        applyScanFilters(rawEntries()),
        entry -> entry != null && entry.status() != ManifestEntry.Status.DELETED);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private CloseableIterable<ManifestEntry<F>> rawEntries() {
    if (directRowsOnly) {
      return (CloseableIterable<ManifestEntry<F>>)
          (CloseableIterable) projector.directDataEntriesFromRoot();
    } else if (manifestContent == ManifestContent.DATA) {
      return (CloseableIterable<ManifestEntry<F>>) (CloseableIterable) projector.dataEntries();
    } else {
      return (CloseableIterable<ManifestEntry<F>>) (CloseableIterable) projector.deleteEntries();
    }
  }

  // Applies partition/metrics/partitionSet filters and skipped-file counters mirrored from the
  // base ManifestReader's chained setters. Column projection (base's select/project) is not
  // replicated because the underlying v4 reader has already decoded rows into full-schema
  // ManifestEntry instances — dropping columns post-hoc would not reduce IO and only complicate
  // downstream file access; consumers requesting columns get the full-schema entry.
  private CloseableIterable<ManifestEntry<F>> applyScanFilters(
      CloseableIterable<ManifestEntry<F>> entries) {
    if (!hasScanFilter()) {
      return entries;
    }

    Evaluator evaluator = evaluator();
    InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();
    return CloseableIterable.filter(
        manifestContent == ManifestContent.DATA
            ? adapterScanMetrics.skippedDataFiles()
            : adapterScanMetrics.skippedDeleteFiles(),
        entries,
        entry ->
            entry != null
                && evaluator.eval(entry.file().partition())
                && metricsEvaluator.eval(entry.file())
                && inPartitionSet(entry.file()));
  }

  @Override
  public CloseableIterator<F> iterator() {
    // Track ordinal position and set both fileOrdinal and manifestLocation on each file so that
    // pos() and manifestLocation() return the expected values, matching the Avro reader behavior.
    // Apply firstRowId assignment following the same logic as ManifestReader.idAssigner(): if a
    // manifest-level firstRowId is present, assign sequential IDs; if the manifest is committed
    // with no firstRowId, nullify per-entry firstRowIds; if uncommitted, leave them as-is.
    return CloseableIterable.transform(
            liveEntries(),
            new java.util.function.Function<ManifestEntry<F>, F>() {
              private long ordinal = 0L;
              private long nextRowId = firstRowId != null ? firstRowId : 0L;

              @Override
              public F apply(ManifestEntry<F> entry) {
                F file = entry.file();
                if (file instanceof BaseFile) {
                  BaseFile<?> baseFile = (BaseFile<?>) file;
                  baseFile.setFileOrdinal(ordinal);
                  baseFile.setManifestLocation(manifestLocation);
                  if (firstRowId != null) {
                    // manifest-level firstRowId overrides per-entry value
                    if (baseFile.firstRowId() == null
                        && entry.status() != ManifestEntry.Status.DELETED) {
                      baseFile.setFirstRowId(nextRowId);
                      nextRowId += baseFile.recordCount();
                    }
                  } else if (isCommitted) {
                    // committed manifest with no manifest-level firstRowId: nullify per-entry value
                    baseFile.setFirstRowId(null);
                  }
                  // else: uncommitted — preserve per-entry firstRowId from tracking struct
                }

                ordinal += 1;
                return file;
              }
            })
        .iterator();
  }
}
