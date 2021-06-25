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

package org.apache.iceberg.flink.source.assigner.ordered;

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects.ToStringHelper;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-Memory version of GlobalWatermarkTracker that can be run on the JobMaster.
 *
 * <p>This particular implementation computes the watermark value on every read - so technically
 * reads can be expensive. However, the reads are still bound by the number of partitions (which we
 * expect to be super low). If this assumption becomes false in the future, we can revisit the
 * implementation to cache the watermark on every write.
 *
 * @param <Partition> type of the partition
 */
class InMemoryGlobalWatermarkTracker<Partition> implements GlobalWatermarkTracker<Partition> {
  private static final Logger log = LoggerFactory.getLogger(InMemoryGlobalWatermarkTracker.class);

  private final ConcurrentMap<Partition, PartitionWatermarkState> acc = new ConcurrentHashMap<>();
  private final ConcurrentMap<Partition, WeakHashMap<WatermarkTracker.Listener, Void>> listeners =
      new ConcurrentHashMap<>();
  private final AtomicReference<Long> globalWatermark = new AtomicReference<>();
  // private final Registry registry;

  // public InMemoryGlobalWatermarkTracker(Registry registry) {
  //   this.registry = new FlinkRegistry(registry, "GlobalWatermarkTracker", ImmutableList.of());
  // }

  // private Gauge getWatermarkGaugeFor(String partition) {
  //   return registry.gauge("watermark", "partition", partition);
  // }

  private Long updateAndGet() {
    return globalWatermark.updateAndGet(
        oldValue -> {
          return acc.values()
              .stream()
              .filter(partitionWatermarkState -> !partitionWatermarkState.isComplete())
              .map(PartitionWatermarkState::getWatermark)
              .min(Comparator.naturalOrder())
              .orElse(null);
        });
  }

  private void updateWatermarkAndInformListeners(Partition updatedPartition) {
    Long v1 = globalWatermark.get();
    Long v2 = updateAndGet();

    if (v1 == null || !v1.equals(v2)) {
      listeners
          .entrySet()
          .stream()
          .filter(entry -> !entry.getKey().equals(updatedPartition))
          .forEach(
              entry -> {
                entry.getValue().keySet().forEach(listener -> listener.onWatermarkChange(v2));
              });
    }

    updateGauges();
  }

  @Nullable
  @Override
  public Long getGlobalWatermark() throws Exception {
    return globalWatermark.get();
  }

  private void updateGauges() {
    try {
      ToStringHelper helper = MoreObjects.toStringHelper(this);
      for (Entry<Partition, PartitionWatermarkState> entry : acc.entrySet()) {
        Partition p = entry.getKey();
        PartitionWatermarkState v = entry.getValue();
        helper = helper.add(p.toString(), v.toString());
        // getWatermarkGaugeFor(p.toString()).set(v.getWatermark());
      }

      Long globalWatermark = getGlobalWatermark();
      helper.add("global", globalWatermark);
      log.info("watermarkState={}", helper.toString());
      // if (globalWatermark != null) {
      //   getWatermarkGaugeFor("global").set(globalWatermark);
      // }
    } catch (Exception e) {
      log.error("Failed to update the gauges", e);
    }
  }

  @Override
  public Long updateWatermarkForPartition(Partition partition, long watermark) throws Exception {
    log.info("Updating watermark tracker to {} for partition {}", watermark, partition);
    acc.compute(
        partition,
        (dontCare, oldState) ->
            PartitionWatermarkState.max(oldState, new PartitionWatermarkState(watermark, false)));
    updateWatermarkAndInformListeners(partition);
    return getGlobalWatermark();
  }

  @Override
  public void onPartitionCompletion(Partition partition) throws Exception {
    log.info("Marking partition {} as complete", partition);
    acc.compute(
        partition,
        (dontCare, oldState) ->
            PartitionWatermarkState.max(
                oldState, new PartitionWatermarkState(Long.MIN_VALUE, true)));
    updateWatermarkAndInformListeners(partition);
  }

  @Override
  public void onPartitionInitialization(Partition partition) {
    acc.remove(partition);
    listeners.remove(partition);
    updateWatermarkAndInformListeners(partition);
  }

  @Override
  public void addListener(Partition partition, WatermarkTracker.Listener listener) {
    listeners.compute(
        partition,
        (dontCare, oldValue) -> {
          if (oldValue != null) {
            oldValue.put(listener, null);
            return oldValue;
          } else {
            WeakHashMap<WatermarkTracker.Listener, Void> l = new WeakHashMap<>();
            l.put(listener, null);
            return l;
          }
        });
  }

  @Override
  public void removeListener(Partition partition, WatermarkTracker.Listener listener) {
    listeners.compute(
        partition,
        (dontCare, oldValue) -> {
          Preconditions.checkArgument(
              oldValue != null && oldValue.containsKey(listener), "listener not found");
          oldValue.remove(listener);

          if (oldValue.isEmpty()) {
            return null;
          } else {
            return oldValue;
          }
        });
  }

  static class PartitionWatermarkState implements Serializable {

    Long watermark;

    boolean isComplete;

    public PartitionWatermarkState(Long watermark, boolean isComplete) {
      this.watermark = watermark;
      this.isComplete = isComplete;
    }

    public Long getWatermark() {
      return watermark;
    }

    public boolean isComplete() {
      return isComplete;
    }

    static PartitionWatermarkState max(PartitionWatermarkState a, PartitionWatermarkState b) {
      if (a == null) {
        return b;
      }
      if (b == null) {
        return a;
      }

      return new PartitionWatermarkState(
          Math.max(a.watermark, b.watermark), a.isComplete || b.isComplete);
    }
  }
}

