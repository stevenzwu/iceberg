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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.V4RootManifestAssembler.PoolKind;
import org.apache.iceberg.V4RootManifestAssembler.SpillResult;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the adaptive-assembly core: pool classification and streaming-spill partitioning.
 */
public class TestV4RootManifestAssembler {

  private static final double AVG_BYTES = 100.0;
  private static final long TARGET = 1000L; // 10 entries fill a leaf

  private static List<Integer> entries(int count) {
    return IntStream.range(0, count).boxed().collect(Collectors.toList());
  }

  @Test
  public void testClassifyDataLive() {
    for (EntryStatus status :
        new EntryStatus[] {EntryStatus.ADDED, EntryStatus.EXISTING, EntryStatus.MODIFIED}) {
      assertThat(V4RootManifestAssembler.classify(FileContent.DATA, status))
          .as("data %s", status)
          .isEqualTo(PoolKind.DATA_LIVE);
    }
  }

  @Test
  public void testClassifyEqualityDeleteLive() {
    for (EntryStatus status :
        new EntryStatus[] {EntryStatus.ADDED, EntryStatus.EXISTING, EntryStatus.MODIFIED}) {
      assertThat(V4RootManifestAssembler.classify(FileContent.EQUALITY_DELETES, status))
          .as("eq-delete %s", status)
          .isEqualTo(PoolKind.EQ_DELETE_LIVE);
    }
  }

  @Test
  public void testClassifyRetirementPools() {
    assertThat(V4RootManifestAssembler.classify(FileContent.DATA, EntryStatus.DELETED))
        .isEqualTo(PoolKind.DATA_DELETED_RETIRE);
    assertThat(V4RootManifestAssembler.classify(FileContent.DATA, EntryStatus.REPLACED))
        .isEqualTo(PoolKind.DATA_REPLACED_RETIRE);
    assertThat(V4RootManifestAssembler.classify(FileContent.EQUALITY_DELETES, EntryStatus.DELETED))
        .isEqualTo(PoolKind.EQ_DELETE_DELETED_RETIRE);
    assertThat(V4RootManifestAssembler.classify(FileContent.EQUALITY_DELETES, EntryStatus.REPLACED))
        .isEqualTo(PoolKind.EQ_DELETE_REPLACED_RETIRE);
  }

  @Test
  public void testClassifyRejectsManifestReference() {
    assertThatThrownBy(
            () -> V4RootManifestAssembler.classify(FileContent.DATA_MANIFEST, EntryStatus.ADDED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Not a direct content entry");
  }

  @Test
  public void testSpillEmptyPool() {
    SpillResult<Integer> result =
        V4RootManifestAssembler.partitionForSpill(entries(0), AVG_BYTES, TARGET);
    assertThat(result.leafBatches()).isEmpty();
    assertThat(result.rootRemainder()).isEmpty();
  }

  @Test
  public void testSpillBelowTargetStaysInRoot() {
    // 5 entries * 100 = 500 < 1000 target -> no leaf, all stay as root-direct rows.
    SpillResult<Integer> result =
        V4RootManifestAssembler.partitionForSpill(entries(5), AVG_BYTES, TARGET);
    assertThat(result.leafBatches()).isEmpty();
    assertThat(result.rootRemainder()).hasSize(5);
  }

  @Test
  public void testSpillExactlyOneLeafNoRemainder() {
    // 10 entries * 100 = 1000 == target -> exactly one leaf, no remainder.
    SpillResult<Integer> result =
        V4RootManifestAssembler.partitionForSpill(entries(10), AVG_BYTES, TARGET);
    assertThat(result.leafBatches()).hasSize(1);
    assertThat(result.leafBatches().get(0)).hasSize(10);
    assertThat(result.rootRemainder()).isEmpty();
  }

  @Test
  public void testSpillFillsLeavesAndKeepsRemainder() {
    // 25 entries -> two full leaves (10 + 10) + 5 trailing entries kept in root.
    SpillResult<Integer> result =
        V4RootManifestAssembler.partitionForSpill(entries(25), AVG_BYTES, TARGET);
    assertThat(result.leafBatches()).hasSize(2);
    assertThat(result.leafBatches()).allSatisfy(batch -> assertThat(batch).hasSize(10));
    assertThat(result.rootRemainder()).hasSize(5);
    // every entry is accounted for exactly once, in arrival order
    List<Integer> flattened =
        result.leafBatches().stream().flatMap(List::stream).collect(Collectors.toList());
    flattened.addAll(result.rootRemainder());
    assertThat(flattened).containsExactlyElementsOf(entries(25));
  }

  @Test
  public void testSpillExactMultipleHasEmptyRemainder() {
    // 20 entries -> two full leaves, empty remainder.
    SpillResult<Integer> result =
        V4RootManifestAssembler.partitionForSpill(entries(20), AVG_BYTES, TARGET);
    assertThat(result.leafBatches()).hasSize(2);
    assertThat(result.rootRemainder()).isEmpty();
  }
}
