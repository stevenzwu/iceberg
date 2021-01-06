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

package org.apache.iceberg.flink.source.enumerator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSourceEvents;
import org.apache.iceberg.flink.source.assigner.GetSplitResult;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.flink.source.assigner.GetSplitResult.Status.AVAILABLE;
import static org.apache.iceberg.flink.source.assigner.GetSplitResult.Status.CONSTRAINED;
import static org.apache.iceberg.flink.source.assigner.GetSplitResult.Status.UNAVAILABLE;

abstract class AbstractIcebergEnumerator implements
    SplitEnumerator<IcebergSourceSplit, IcebergEnumState> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractIcebergEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumContext;
  private final SplitAssigner assigner;
  private final Map<Integer, String> readersAwaitingSplit;
  private final AtomicReference<CompletableFuture<Void>> availableFuture;

  AbstractIcebergEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      SplitAssigner assigner) {
    this.enumContext = enumContext;
    this.assigner = assigner;
    this.readersAwaitingSplit = new LinkedHashMap<>();
    this.availableFuture = new AtomicReference<>();
  }

  @Override
  public void start() {
    assigner.start();
  }

  @Override
  public void close() throws IOException {
    assigner.close();
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // Iceberg source uses a custom event inside handleSourceEvent
    // so that we can carry over the finishedSplitIds
    throw new UnsupportedOperationException("Iceberg source uses its own SplitRequestEvent");
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    if (sourceEvent instanceof IcebergSourceEvents.SplitRequestEvent) {
      final IcebergSourceEvents.SplitRequestEvent splitRequestEvent =
          (IcebergSourceEvents.SplitRequestEvent) sourceEvent;
      LOG.error("Received request split event from subtask {}", subtaskId);
      assigner.onCompletedSplits(splitRequestEvent.finishedSplitIds(), subtaskId);
      readersAwaitingSplit.put(subtaskId, splitRequestEvent.requesterHostname());
      assignSplits();
    } else {
      LOG.error("Received unrecognized event from subtask {}: {}", subtaskId, sourceEvent);
    }
  }

  @Override
  public void addSplitsBack(List<IcebergSourceSplit> splits, int subtaskId) {
    LOG.info("Add {} splits back to the pool for failed subtask {}: {}",
        splits.size(), subtaskId, splits);
    assigner.onUnassignedSplits(splits, subtaskId);
    assignSplits();
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("Added reader {}", subtaskId);
  }

  protected Table loadTable(TableLoader tableLoader) {
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      return loader.loadTable();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close table loader", e);
    }
  }

  private void assignSplits() {
    final Iterator<Map.Entry<Integer, String>> awaitingReader =
        readersAwaitingSplit.entrySet().iterator();
    while (awaitingReader.hasNext()) {
      final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();
      // if the reader that requested another split has failed in the meantime, remove
      // it from the list of waiting readers
      if (!enumContext.registeredReaders().containsKey(nextAwaiting.getKey())) {
        awaitingReader.remove();
        continue;
      }

      final String hostname = nextAwaiting.getValue();
      final int awaitingSubtask = nextAwaiting.getKey();
      final GetSplitResult getResult = assigner.getNext(hostname);
      if (getResult.status() == AVAILABLE) {
        enumContext.assignSplit(getResult.split(), awaitingSubtask);
        awaitingReader.remove();
      } else if (getResult.status() == CONSTRAINED) {
        getAvailableFutureIfNeeded();
        break;
      } else if (getResult.status() == UNAVAILABLE) {
        final boo
      } else {
      }
    }
  }

  /**
   * @return true if more splits can be available later
   * like in the continuous enumerator case
   */
  protected abstract boolean handleUnavailable();

  private synchronized void getAvailableFutureIfNeeded() {
    if (availableFuture.get() != null) {
      return;
    }
    CompletableFuture<Void> future = assigner.isAvailable();
    future.thenAccept(ignore ->
        // Must run assignSplits in coordinator thread
        // because the future may be completed from other threads.
        // E.g., in event time alignment assigner,
        // watermark advancement from another source may
        // cause the available future to be completed
        enumContext.runInCoordinatorThread(() -> assignSplits()));
    availableFuture.set(future);
  }
}
