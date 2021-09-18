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

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.assigner.SplitAssignerFactory;
import org.apache.iceberg.flink.source.enumerator.ContinuousIcebergEnumerator;
import org.apache.iceberg.flink.source.enumerator.ContinuousSplitPlanner;
import org.apache.iceberg.flink.source.enumerator.ContinuousSplitPlannerImpl;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorConfig;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorState;
import org.apache.iceberg.flink.source.enumerator.IcebergEnumeratorStateSerializer;
import org.apache.iceberg.flink.source.enumerator.StaticIcebergEnumerator;
import org.apache.iceberg.flink.source.reader.IcebergSourceReader;
import org.apache.iceberg.flink.source.reader.IcebergSourceReaderMetrics;
import org.apache.iceberg.flink.source.reader.ReaderFunction;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
public class IcebergSource<T> implements Source<T, IcebergSourceSplit, IcebergEnumeratorState> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSource.class);

  private final TableLoader tableLoader;
  private final ScanContext scanContext;
  private final ReaderFunction<T> readerFunction;
  private final SplitAssignerFactory assignerFactory;
  private final IcebergEnumeratorConfig enumeratorConfig;

  IcebergSource(
      TableLoader tableLoader,
      ScanContext scanContext,
      ReaderFunction<T> readerFunction,
      SplitAssignerFactory assignerFactory,
      IcebergEnumeratorConfig enumeratorConfig) {

    this.tableLoader = tableLoader;
    this.enumeratorConfig = enumeratorConfig;
    this.scanContext = scanContext;
    this.readerFunction = readerFunction;
    this.assignerFactory = assignerFactory;
  }

  private static Table loadTable(TableLoader tableLoader) {
    tableLoader.open();
    try (TableLoader loader = tableLoader) {
      return loader.loadTable();
    } catch (IOException e) {
      throw new RuntimeException("Failed to close table loader", e);
    }
  }

  @Override
  public Boundedness getBoundedness() {
    return enumeratorConfig.splitDiscoveryInterval() == null ?
        Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<T, IcebergSourceSplit> createReader(SourceReaderContext readerContext) {
    final IcebergSourceReaderMetrics readerMetrics = new IcebergSourceReaderMetrics(readerContext.metricGroup());
    return new IcebergSourceReader<>(
        readerFunction,
        readerContext,
        readerMetrics);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext) {
    return createEnumerator(enumContext, null);
  }

  @Override
  public SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext, IcebergEnumeratorState enumState) {
    return createEnumerator(enumContext, enumState);
  }

  @Override
  public SimpleVersionedSerializer<IcebergSourceSplit> getSplitSerializer() {
    return IcebergSourceSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<IcebergEnumeratorState> getEnumeratorCheckpointSerializer() {
    return IcebergEnumeratorStateSerializer.INSTANCE;
  }

  private SplitEnumerator<IcebergSourceSplit, IcebergEnumeratorState> createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumContext,
      @Nullable IcebergEnumeratorState enumState) {

    final Table table = loadTable(tableLoader);

    final SplitAssigner assigner;
    if (enumState == null) {
      assigner = assignerFactory.createAssigner();
    } else {
      LOG.info("Iceberg source restored {} splits from state for table {}",
          enumState.pendingSplits().size(), table.name());
      assigner = assignerFactory.createAssigner(enumState.pendingSplits());
    }

    if (enumeratorConfig.splitDiscoveryInterval() == null) {
      return new StaticIcebergEnumerator(enumContext, assigner, table, scanContext, enumState);
    } else {
      final ContinuousSplitPlanner splitPlanner = new ContinuousSplitPlannerImpl(
          table, enumeratorConfig, scanContext);
      return new ContinuousIcebergEnumerator(enumContext, assigner, enumeratorConfig, splitPlanner, enumState);
    }
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static class Builder<T> {

    // required
    private TableLoader tableLoader;
    private SplitAssignerFactory splitAssignerFactory;
    private ReaderFunction<T> readerFunction;

    // optional
    private ScanContext scanContext;
    private IcebergEnumeratorConfig enumeratorConfig;

    Builder() {
      this.scanContext = ScanContext.builder().build();
      this.enumeratorConfig = IcebergEnumeratorConfig.builder().build();
    }

    public Builder<T> tableLoader(TableLoader loader) {
      this.tableLoader = loader;
      return this;
    }

    public Builder<T> assignerFactory(SplitAssignerFactory assignerFactory) {
      this.splitAssignerFactory = assignerFactory;
      return this;
    }

    public Builder<T> readerFunction(ReaderFunction<T> newReaderFunction) {
      this.readerFunction = newReaderFunction;
      return this;
    }

    public Builder<T> scanContext(ScanContext newScanContext) {
      this.scanContext = newScanContext;
      return this;
    }

    public Builder<T> enumeratorConfig(IcebergEnumeratorConfig newConfig) {
      this.enumeratorConfig = newConfig;
      return this;
    }

    public IcebergSource<T> build() {
      checkRequired();
      return new IcebergSource<>(
          tableLoader,
          scanContext,
          readerFunction,
          splitAssignerFactory,
          enumeratorConfig);
    }

    private void checkRequired() {
      Preconditions.checkNotNull(tableLoader, "tableLoader is required.");
      Preconditions.checkNotNull(splitAssignerFactory, "asignerFactory is required.");
      Preconditions.checkNotNull(readerFunction, "bulkFormat is required.");
    }
  }
}
