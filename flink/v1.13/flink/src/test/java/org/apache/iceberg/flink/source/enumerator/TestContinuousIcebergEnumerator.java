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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.iceberg.flink.source.IcebergSourceEvents;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.flink.source.split.SplitHelpers;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestContinuousIcebergEnumerator {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Test
  public void testDiscoverSplitWhenNoReaderRegistered() throws Exception {
    TestContinuousSplitPlanner splitPlanner = new TestContinuousSplitPlanner();
    TestingSplitEnumeratorContext<IcebergSourceSplit> context =
        new TestingSplitEnumeratorContext<>(4);
    IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    ContinuousIcebergEnumerator enumerator = createEnumerator(context, config, splitPlanner);

    Collection<IcebergSourceSplitState> pendingSplitsEmpty = enumerator.snapshotState(1).pendingSplits();
    Assert.assertEquals(0, pendingSplitsEmpty.size());

    // make one split available and trigger the periodic discovery
    List<IcebergSourceSplit> splits = SplitHelpers
        .createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    splitPlanner.addSplits(splits);
    context.triggerAllActions();

    Collection<IcebergSourceSplitState> pendingSplits = enumerator.snapshotState(2).pendingSplits();
    Assert.assertEquals(1, pendingSplits.size());
    IcebergSourceSplitState pendingSplit = pendingSplits.iterator().next();
    Assert.assertEquals(splits.get(0).splitId(), pendingSplit.split().splitId());
    Assert.assertEquals(IcebergSourceSplitStatus.UNASSIGNED, pendingSplit.status());
  }

  @Test
  public void testDiscoverWhenReaderRegistered() throws Exception {
    TestContinuousSplitPlanner splitPlanner = new TestContinuousSplitPlanner();
    TestingSplitEnumeratorContext<IcebergSourceSplit> context =
        new TestingSplitEnumeratorContext<>(4);
    IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    ContinuousIcebergEnumerator enumerator = createEnumerator(context, config, splitPlanner);

    // register one reader, and let it request a split
    context.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2,
        new IcebergSourceEvents.SplitRequestEvent());

    // make one split available and trigger the periodic discovery
    List<IcebergSourceSplit> splits = SplitHelpers
        .createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    splitPlanner.addSplits(splits);
    context.triggerAllActions();

    Assert.assertTrue(enumerator.snapshotState(1).pendingSplits().isEmpty());
    MatcherAssert.assertThat(context.getSplitAssignments().get(2).getAssignedSplits(),
        CoreMatchers.hasItem(splits.get(0)));
  }

  @Test
  public void testRequestingReaderUnavailableWhenSplitDiscovered() throws Exception {
    TestContinuousSplitPlanner splitPlanner = new TestContinuousSplitPlanner();
    TestingSplitEnumeratorContext<IcebergSourceSplit> context =
        new TestingSplitEnumeratorContext<>(4);
    IcebergEnumeratorConfig config = IcebergEnumeratorConfig.builder()
        .splitDiscoveryInterval(Duration.ofMinutes(5L))
        .startingStrategy(IcebergEnumeratorConfig.StartingStrategy.TABLE_SCAN_THEN_INCREMENTAL)
        .build();
    ContinuousIcebergEnumerator enumerator = createEnumerator(context, config, splitPlanner);

    // register one reader, and let it request a split
    context.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2,
        new IcebergSourceEvents.SplitRequestEvent());

    // remove the reader (like in a failure)
    context.registeredReaders().remove(2);

    // make one split available and trigger the periodic discovery
    List<IcebergSourceSplit> splits = SplitHelpers
        .createSplitsFromTransientHadoopTable(TEMPORARY_FOLDER, 1, 1);
    Assert.assertEquals(1, splits.size());
    splitPlanner.addSplits(splits);
    context.triggerAllActions();

    Assert.assertFalse(context.getSplitAssignments().containsKey(2));
    List<String> pendingSplitIds = enumerator.snapshotState(1).pendingSplits().stream()
        .map(IcebergSourceSplitState::split)
        .map(IcebergSourceSplit::splitId)
        .collect(Collectors.toList());
    Assert.assertEquals(splits.size(), pendingSplitIds.size());
    Assert.assertEquals(splits.get(0).splitId(), pendingSplitIds.get(0));

    // register the reader again, and let it request a split
    context.registerReader(2, "localhost");
    enumerator.addReader(2);
    enumerator.handleSourceEvent(2,
        new IcebergSourceEvents.SplitRequestEvent());

    Assert.assertTrue(enumerator.snapshotState(2).pendingSplits().isEmpty());
    MatcherAssert.assertThat(context.getSplitAssignments().get(2).getAssignedSplits(),
        CoreMatchers.hasItem(splits.get(0)));
  }

  private static ContinuousIcebergEnumerator createEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> context,
      IcebergEnumeratorConfig config,
      ContinuousSplitPlanner splitPlanner) {

    ContinuousIcebergEnumerator enumerator =
        new ContinuousIcebergEnumerator(
            context,
            new SimpleSplitAssigner(Collections.emptyList()),
            config,
            splitPlanner,
            null);
    enumerator.start();
    return enumerator;
  }

}
