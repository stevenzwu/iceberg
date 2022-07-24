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

import java.util.Arrays;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A default {@link Histogram} implementation that uses a fixed size reservoir
 * like circular buffer. Oldest observation is replaced by latest observation.
 */
public class DefaultHistogram implements Histogram {
  private final long[] measurements;
  private long count;

  public DefaultHistogram(int reservoirSize) {
    this.measurements = new long[reservoirSize];
    this.count = 0L;
  }

  @Override
  public synchronized long count() {
    return count;
  }

  @Override
  public synchronized void update(long value) {
    int index = (int) (count % measurements.length);
    measurements[index] = value;
    count++;
  }

  @Override
  public Statistics statistics() {
    int size = measurements.length;
    if (count < (long) measurements.length) {
      size = (int) count;
    }

    long[] values = new long[size];
    for (int i = 0; i < values.length; ++i) {
      synchronized (this) {
        values[i] = measurements[i];
      }
    }

    return new UniformWeightStatistics(values);
  }

  private static class UniformWeightStatistics implements Statistics {
    private final long[] values;

    private UniformWeightStatistics(long[] values) {
      // since the input values is already a copied array,
      // there is no need to copy again.
      this.values = values;
      Arrays.sort(this.values);
    }

    @Override
    public long size() {
      return values.length;
    }

    @Override
    public double mean() {
      if (values.length == 0) {
        return 0.0;
      } else {
        double sum = 0.0;
        for (int i = 0; i < values.length; ++i) {
          sum += values[i];
        }

        return sum / values.length;
      }
    }

    @Override
    public double stdDev() {
      if (this.values.length <= 1) {
        return 0.0;
      } else {
        double mean = mean();
        double sumOfDiffSquares = 0.0;
        for (int i = 0; i < values.length; ++i) {
          double diff = values[i] - mean;
          sumOfDiffSquares += diff * diff;
        }

        double variance = sumOfDiffSquares / values.length;
        return Math.sqrt(variance);
      }
    }

    @Override
    public long max() {
      return values.length == 0 ? 0L : values[values.length - 1];
    }

    @Override
    public long min() {
      return values.length == 0 ? 0L : values[0];
    }

    @Override
    public double percentile(double percentile) {
      Preconditions.checkArgument(!Double.isNaN(percentile) && percentile >= 0.0 && percentile <= 1.0,
          "Percentile point cannot be outside the range of [0.0 - 1.10]: %s", percentile);
      if (values.length == 0) {
        return 0.0;
      } else {
        double position = percentile * values.length;
        int index = (int) position;
        if (index < 1) {
          return values[0];
        } else if (index >= values.length) {
          return values[values.length - 1];
        } else {
          double lower = (double) values[index - 1];
          double upper = (double) values[index];
          return lower + (position - Math.floor(position)) * (upper - lower);
        }
      }
    }
  }
}
