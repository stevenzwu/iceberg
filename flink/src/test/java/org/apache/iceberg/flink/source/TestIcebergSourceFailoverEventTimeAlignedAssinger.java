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

package org.apache.iceberg.flink.source;

import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopTableResource;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.assigner.SplitAssignerFactory;
import org.apache.iceberg.flink.source.assigner.ordered.ClockFactory;
import org.apache.iceberg.flink.source.assigner.ordered.EventTimeAlignmentAssignerFactory;
import org.apache.iceberg.flink.source.assigner.ordered.InMemoryGlobalWatermarkTrackerFactory;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

public class TestIcebergSourceFailoverEventTimeAlignedAssinger extends TestIcebergSourceFailover {
  // increment ts by 60 minutes for each generateRecords batch
  private static final long RECORD_BATCH_TS_INCREMENT_MILLI = TimeUnit.MINUTES.toMillis(15);
  // Within a batch, increment ts by 1 minute
  private static final long RECORD_TS_INCREMENT_MILLI = TimeUnit.SECONDS.toMillis(1);
  // max out-of-orderliness threshold is 30 minutes
  private static final Duration MAX_OUT_OF_ORDERLINESS_THRESHOLD_DURATION = Duration.ofMinutes(15);

  private final AtomicLong tsMilli = new AtomicLong(System.currentTimeMillis());

  @Override
  protected ScanContext scanContext() {
    return ScanContext.builder()
        .project(TestFixtures.TS_SCHEMA)
        .includeColumnStats(true)
        // essentially force one file per CombinedScanTask from split planning
        .splitOpenFileCost(256 * 1024 * 1024L)
        .build();
  }

  @Override
  protected HadoopTableResource hadoopTableResource() {
    return new HadoopTableResource(TEMPORARY_FOLDER,
        TestFixtures.DATABASE, TestFixtures.TABLE, TestFixtures.TS_SCHEMA, TestFixtures.TS_SPEC);
  }

  @Override
  protected Schema schema() {
    return TestFixtures.TS_SCHEMA;
  }

  @Override
  protected RowType rowType() {
    return TestFixtures.TS_ROW_TYPE;
  }

  @Override
  protected List<Record> generateRecords(int numRecords, long seed) {
    tsMilli.addAndGet(RECORD_BATCH_TS_INCREMENT_MILLI);
    return RandomGenericData.generate(schema(), numRecords, seed)
        .stream()
        .map(record -> {
          LocalDateTime ts = LocalDateTime.ofInstant(
              Instant.ofEpochMilli(tsMilli.addAndGet(RECORD_TS_INCREMENT_MILLI)), ZoneId.of("Z"));
          record.setField("ts", ts);
          return record;
        })
        .collect(Collectors.toList());
  }

  @Override
  protected SplitAssignerFactory assignerFactory() {
    return  new EventTimeAlignmentAssignerFactory(
        TestFixtures.TABLE,
        MAX_OUT_OF_ORDERLINESS_THRESHOLD_DURATION,
        (ClockFactory) () -> Clock.systemUTC(),
        new InMemoryGlobalWatermarkTrackerFactory(),
        new IcebergSourceSplitTimeAssigner(TestFixtures.TS_SCHEMA, "ts"));
  }

  private static class IcebergSourceSplitTimeAssigner implements Serializable, TimestampAssigner<IcebergSourceSplit> {
    private final int tsFieldId;

    IcebergSourceSplitTimeAssigner(Schema schema, String tsFieldName) {
      this.tsFieldId = schema.findField(tsFieldName).fieldId();
    }

    @Override
    public long extractTimestamp(IcebergSourceSplit split, long recordTimestamp) {
      return split.task().files().stream()
          .map(scanTask -> (long) Conversions.fromByteBuffer(
              Types.LongType.get(), scanTask.file().lowerBounds().get(tsFieldId)) / 1000L)
          .min(Comparator.comparingLong(l -> l))
          .get();
    }
  }
}
