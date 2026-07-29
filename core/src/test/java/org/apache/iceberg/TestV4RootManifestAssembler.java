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

import org.apache.iceberg.V4RootManifestAssembler.PoolKind;
import org.junit.jupiter.api.Test;

/** Unit tests for the adaptive-assembly pool classification. */
public class TestV4RootManifestAssembler {

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
}
