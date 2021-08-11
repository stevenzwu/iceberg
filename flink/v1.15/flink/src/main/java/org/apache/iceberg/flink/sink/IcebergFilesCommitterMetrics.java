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

package org.apache.iceberg.flink.sink;

import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.flink.FlinkMetricsContext;
import org.apache.iceberg.metrics.Gauge;
import org.apache.iceberg.metrics.MetricsContext;

class IcebergFilesCommitterMetrics implements MetricsContext {
  private final Gauge<Long> lastCheckpointDurationMs;
  private final Gauge<Long> lastCommitDurationMs;
  private final Counter<Long> committedDataFilesCount;
  private final Counter<Long> committedDataFilesRecordCount;
  private final Counter<Long> committedDataFilesByteCount;
  private final Counter<Long> committedDeleteFilesCount;
  private final Counter<Long> committedDeleteFilesRecordCount;
  private final Counter<Long> committedDeleteFilesByteCount;

  IcebergFilesCommitterMetrics(MetricGroup metrics, String fullTableName) {
    FlinkMetricsContext metricsContext = new FlinkMetricsContext(metrics, "IcebergFilesCommitter", fullTableName);
    this.lastCheckpointDurationMs = metricsContext.gauge("lastCheckpointDurationMs", Long.class);
    this.lastCommitDurationMs = metricsContext.gauge("lastCommitDurationMs", Long.class);
    this.committedDataFilesCount = metricsContext.counter("committedDataFilesCount", Long.class, Unit.COUNT);
    this.committedDataFilesRecordCount = metricsContext.counter(
        "committedDataFilesRecordCount",  Long.class, Unit.COUNT);
    this.committedDataFilesByteCount = metricsContext.counter(
        "committedDataFilesByteCount",  Long.class, Unit.COUNT);
    this.committedDeleteFilesCount = metricsContext.counter("committedDeleteFilesCount",  Long.class, Unit.COUNT);
    this.committedDeleteFilesRecordCount = metricsContext.counter(
        "committedDeleteFilesRecordCount",  Long.class, Unit.COUNT);
    this.committedDeleteFilesByteCount = metricsContext.counter(
        "committedDeleteFilesByteCount",  Long.class, Unit.COUNT);
  }

  void checkpointDuration(long checkpointDurationMs) {
    lastCheckpointDurationMs.set(checkpointDurationMs);
  }

  void commitDuration(long commitDurationMs) {
    lastCommitDurationMs.set(commitDurationMs);
  }

  void updateCommitStats(CommitStats stats) {
    committedDataFilesCount.increment(stats.dataFilesCount());
    committedDataFilesRecordCount.increment(stats.dataFilesRecordCount());
    committedDataFilesByteCount.increment(stats.dataFilesByteCount());
    committedDeleteFilesCount.increment(stats.deleteFilesCount());
    committedDeleteFilesRecordCount.increment(stats.deleteFilesRecordCount());
    committedDeleteFilesByteCount.increment(stats.deleteFilesByteCount());
  }
}
