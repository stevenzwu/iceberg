/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import java.util.Arrays;
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.flink.FlinkMetricsContext;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.metrics.Gauge;
import org.apache.iceberg.metrics.Histogram;
import org.apache.iceberg.metrics.MetricsContext;

class IcebergStreamWriterMetrics {
  // 1,024 reservoir size should cost about 8KB, which is quite small.
  // It should also produce good accuracy for histogram distribution (like percentiles).
  private static final int HISTOGRAM_RESERVOIR_SIZE = 1024;

  private final MetricsContext.Counter<Long> flushedDataFiles;
  private final MetricsContext.Counter<Long> flushedDeleteFiles;
  private final MetricsContext.Counter<Long> flushedReferencedDataFiles;
  private final Gauge<Long> lastFlushDurationMs;
  private final Histogram dataFilesSizeHistogram;
  private final Histogram deleteFilesSizeHistogram;

  IcebergStreamWriterMetrics(MetricGroup metrics, String fullTableName) {
    FlinkMetricsContext metricsContext = new FlinkMetricsContext(metrics, "IcebergStreamWriter", fullTableName);
    this.flushedDataFiles = metricsContext.counter("flushedDataFiles", Long.class, MetricsContext.Unit.COUNT);
    this.flushedDeleteFiles = metricsContext.counter("flushedDeleteFiles", Long.class, MetricsContext.Unit.COUNT);
    this.flushedReferencedDataFiles = metricsContext.counter(
        "flushedReferencedDataFiles", Long.class, MetricsContext.Unit.COUNT);
    this.lastFlushDurationMs = metricsContext.gauge("lastFlushDurationMs", Long.class);

    this.dataFilesSizeHistogram = metricsContext.histogram("dataFilesSizeHistogram", HISTOGRAM_RESERVOIR_SIZE);
    this.deleteFilesSizeHistogram = metricsContext.histogram("deleteFilesSizeHistogram", HISTOGRAM_RESERVOIR_SIZE);
  }

  void updateFlushResult(WriteResult result) {
    flushedDataFiles.increment((long) result.dataFiles().length);
    flushedDeleteFiles.increment((long) result.deleteFiles().length);
    flushedReferencedDataFiles.increment((long) result.referencedDataFiles().length);

    // Update file size distribution histogram after flush. It is not important to publish committed files.
    // This should work equally well. This avoids the overhead of tracking the list of file sizes
    // in the CommitStats, which currently stores simple stats for counters and gauges metrics.
    Arrays.stream(result.dataFiles()).forEach(dataFile ->
        dataFilesSizeHistogram.update(dataFile.fileSizeInBytes()));
    Arrays.stream(result.deleteFiles()).forEach(deleteFile ->
        deleteFilesSizeHistogram.update(deleteFile.fileSizeInBytes()));
  }

  void flushDuration(long flushDurationMs) {
    lastFlushDurationMs.set(flushDurationMs);
  }
}
