/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink;

import com.codahale.metrics.SlidingWindowReservoir;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.iceberg.metrics.Gauge;
import org.apache.iceberg.metrics.Histogram;
import org.apache.iceberg.metrics.MetricsContext;

public class FlinkMetricsContext implements MetricsContext {
  private final MetricGroup metrics;

  public FlinkMetricsContext(MetricGroup metrics, String groupName, String fullTableName) {
    this.metrics = metrics.addGroup(groupName, fullTableName);
  }

  @Override
  public <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
    if (type == Long.class) {
      AtomicLong count = new AtomicLong();
      metrics.gauge(name, count::get);
      return (Counter<T>) longCounter(count::addAndGet, count::get);
    } else {
      throw new UnsupportedOperationException("Unsupported counter type: " + type.getCanonicalName());
    }
  }

  @Override
  public <T extends Number> Gauge<T> gauge(String name, Class<T> type) {
    if (type == Long.class) {
      AtomicLong value = new AtomicLong();
      metrics.gauge(name, value::get);
      return (Gauge<T>) longGauge(value::set, value::get);
    } else {
      throw new UnsupportedOperationException("Unsupported gauge type: " + type.getCanonicalName());
    }
  }

  @Override
  public Histogram histogram(String name, int reservoirSize) {
    com.codahale.metrics.Histogram dropwizardHistogram =
        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(reservoirSize));
    org.apache.flink.metrics.Histogram flinkHistogram = metrics.histogram(name,
        new DropwizardHistogramWrapper(dropwizardHistogram));
    return new FlinkHistogramAdaptor(flinkHistogram);
  }

  private Counter<Long> longCounter(Consumer<Long> consumer, Supplier<Long> supplier) {
    return  new Counter<Long>() {
      @Override
      public void increment() {
        increment(1L);
      }

      @Override
      public void increment(Long amount) {
        consumer.accept(amount);
      }

      @Override
      public Long value() {
        return supplier.get();
      }
    };
  }

  private Gauge<Long> longGauge(Consumer<Long> consumer, Supplier<Long> supplier) {
    return new Gauge<Long>() {
      @Override
      public void set(Long value) {
        consumer.accept(value);
      }

      @Override
      public Long get() {
        return supplier.get();
      }
    };
  }

  private static class FlinkHistogramAdaptor implements Histogram {
    private final org.apache.flink.metrics.Histogram flinkHistogram;

    FlinkHistogramAdaptor(org.apache.flink.metrics.Histogram flinkHistogram) {
      this.flinkHistogram = flinkHistogram;
    }

    @Override
    public void update(long value) {
      flinkHistogram.update(value);
    }

    @Override
    public long count() {
      return flinkHistogram.getCount();
    }

    @Override
    public Statistics statistics() {
      return new FlinkHistogramStatistics(flinkHistogram.getStatistics());
    }
  }

  private static class FlinkHistogramStatistics implements Histogram.Statistics {
    private final org.apache.flink.metrics.HistogramStatistics flinkHistogramStatistics;

    FlinkHistogramStatistics(org.apache.flink.metrics.HistogramStatistics flinkHistogramStatistics) {
      this.flinkHistogramStatistics = flinkHistogramStatistics;
    }

    @Override
    public long size() {
      return flinkHistogramStatistics.size();
    }

    @Override
    public double mean() {
      return flinkHistogramStatistics.getMean();
    }

    @Override
    public double stdDev() {
      return flinkHistogramStatistics.getStdDev();
    }

    @Override
    public long max() {
      return flinkHistogramStatistics.getMax();
    }

    @Override
    public long min() {
      return flinkHistogramStatistics.getMin();
    }

    @Override
    public double percentile(double percentile) {
      return flinkHistogramStatistics.getQuantile(percentile);
    }
  }
}
