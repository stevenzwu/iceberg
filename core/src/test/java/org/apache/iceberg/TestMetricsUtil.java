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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestMetricsUtil {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "value", Types.LongType.get()));

  @Test
  public void nullFileReturnsNull() {
    assertThat(MetricsUtil.fromContentFile(SCHEMA, null)).isNull();
  }

  @Test
  public void roundTripAllStats() {
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("file.parquet")
            .withFileSizeInBytes(1024)
            .withMetrics(
                new Metrics(
                    10L,
                    ImmutableMap.of(1, 100L, 2, 200L, 3, 300L),
                    ImmutableMap.of(1, 10L, 2, 8L, 3, 5L),
                    ImmutableMap.of(1, 0L, 2, 2L, 3, 1L),
                    ImmutableMap.of(3, 0L),
                    ImmutableMap.of(
                        1,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 1),
                        3,
                        Conversions.toByteBuffer(Types.LongType.get(), 100L)),
                    ImmutableMap.of(
                        1,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 99),
                        3,
                        Conversions.toByteBuffer(Types.LongType.get(), 999L))))
            .build();

    ContentStats stats = MetricsUtil.fromContentFile(SCHEMA, file);

    assertThat(stats).isNotNull();

    FieldStats<Integer> idStats = stats.statsFor(1);
    assertThat(idStats).isNotNull();
    assertThat(idStats.valueCount()).isEqualTo(10L);
    assertThat(idStats.nullValueCount()).isEqualTo(0L);
    assertThat(idStats.lowerBound()).isEqualTo(1);
    assertThat(idStats.upperBound()).isEqualTo(99);

    FieldStats<Long> valueStats = stats.statsFor(3);
    assertThat(valueStats).isNotNull();
    assertThat(valueStats.valueCount()).isEqualTo(5L);
    assertThat(valueStats.nullValueCount()).isEqualTo(1L);
    assertThat(valueStats.nanValueCount()).isEqualTo(0L);
    assertThat(valueStats.lowerBound()).isEqualTo(100L);
    assertThat(valueStats.upperBound()).isEqualTo(999L);

    FieldStats<CharSequence> nameStats = stats.statsFor(2);
    assertThat(nameStats).isNotNull();
    assertThat(nameStats.valueCount()).isEqualTo(8L);
    assertThat(nameStats.nullValueCount()).isEqualTo(2L);
    assertThat(nameStats.lowerBound()).isNull();
    assertThat(nameStats.upperBound()).isNull();
  }

  @Test
  public void schemaEvolutionSkipsUnknownFieldIds() {
    // field id 99 is not in SCHEMA -- it should be silently dropped
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("file.parquet")
            .withFileSizeInBytes(512)
            .withMetrics(
                new Metrics(
                    5L,
                    null,
                    null,
                    null,
                    null,
                    ImmutableMap.of(
                        1,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 0),
                        99,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 42)),
                    ImmutableMap.of(
                        1,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 100),
                        99,
                        Conversions.toByteBuffer(Types.IntegerType.get(), 42))))
            .build();

    ContentStats stats = MetricsUtil.fromContentFile(SCHEMA, file);

    assertThat(stats).isNotNull();
    FieldStats<Integer> idStats = stats.statsFor(1);
    assertThat(idStats).isNotNull();
    assertThat(idStats.lowerBound()).isEqualTo(0);
    assertThat(idStats.upperBound()).isEqualTo(100);
    assertThat(stats.statsFor(99)).isNull();
  }

  @Test
  public void emptyBoundsMapsProduceCountStats() {
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("file.parquet")
            .withFileSizeInBytes(256)
            .withMetrics(
                new Metrics(
                    7L, null, ImmutableMap.of(1, 7L, 2, 7L), ImmutableMap.of(1, 0L, 2, 3L), null))
            .build();

    ContentStats stats = MetricsUtil.fromContentFile(SCHEMA, file);

    assertThat(stats).isNotNull();

    FieldStats<Integer> idStats = stats.statsFor(1);
    assertThat(idStats).isNotNull();
    assertThat(idStats.valueCount()).isEqualTo(7L);
    assertThat(idStats.nullValueCount()).isEqualTo(0L);
    assertThat(idStats.lowerBound()).isNull();
    assertThat(idStats.upperBound()).isNull();

    FieldStats<CharSequence> nameStats = stats.statsFor(2);
    assertThat(nameStats).isNotNull();
    assertThat(nameStats.valueCount()).isEqualTo(7L);
    assertThat(nameStats.nullValueCount()).isEqualTo(3L);
  }
}
