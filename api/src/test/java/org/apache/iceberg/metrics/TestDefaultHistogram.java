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
package org.apache.iceberg.metrics;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestDefaultHistogram {
  @Test
  public void emptyHistogram() {
    DefaultHistogram histogram = new DefaultHistogram(128);
    Assertions.assertThat(histogram.count()).isEqualTo(0L);
    Histogram.Statistics statistics = histogram.statistics();
    Assertions.assertThat(statistics.size()).isEqualTo(0L);
    Assertions.assertThat(statistics.mean()).isEqualTo(0.0);
    Assertions.assertThat(statistics.stdDev()).isEqualTo(0.0);
    Assertions.assertThat(statistics.max()).isEqualTo(0L);
    Assertions.assertThat(statistics.min()).isEqualTo(0L);
    Assertions.assertThat(statistics.percentile(0.50)).isEqualTo(0L);
    Assertions.assertThat(statistics.percentile(0.99)).isEqualTo(0L);
  }

  @Test
  public void countLessThanReservoirSize() {
    DefaultHistogram histogram = new DefaultHistogram(128);
    histogram.update(123L);
    Assertions.assertThat(histogram.count()).isEqualTo(1L);
    Histogram.Statistics statistics = histogram.statistics();
    Assertions.assertThat(statistics.size()).isEqualTo(1L);
    Assertions.assertThat(statistics.mean()).isEqualTo(123);
    Assertions.assertThat(statistics.stdDev()).isEqualTo(0.0);
    Assertions.assertThat(statistics.max()).isEqualTo(123L);
    Assertions.assertThat(statistics.min()).isEqualTo(123L);
    Assertions.assertThat(statistics.percentile(0.50)).isEqualTo(123L);
    Assertions.assertThat(statistics.percentile(0.99)).isEqualTo(123L);
  }

  @Test
  public void minMaxPercentilePoints() {
    int reservoirSize = 128;
    DefaultHistogram histogram = new DefaultHistogram(reservoirSize);
    for (int i = 0; i < reservoirSize; ++i) {
      histogram.update(i);
    }

    Histogram.Statistics statistics = histogram.statistics();
    Assertions.assertThat(statistics.percentile(0.0)).isEqualTo(0.0);
    Assertions.assertThat(statistics.percentile(1.0)).isEqualTo(127.0);
  }

  @Test
  public void invalidPercentilePoints() {
    int reservoirSize = 128;
    DefaultHistogram histogram = new DefaultHistogram(reservoirSize);
    for (int i = 0; i < reservoirSize; ++i) {
      histogram.update(i);
    }

    Histogram.Statistics statistics = histogram.statistics();

    Assertions.assertThatThrownBy(() -> statistics.percentile(-0.1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Percentile point cannot be outside the range of [0.0 - 1.10]: " + -0.1);

    Assertions.assertThatThrownBy(() -> statistics.percentile(1.1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Percentile point cannot be outside the range of [0.0 - 1.10]: " + 1.1);
  }
}
