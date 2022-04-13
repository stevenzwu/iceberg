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

/**
 * Starting strategy for streaming execution.
 */
public enum StreamingStartingStrategy {
  /**
   * First do a regular table scan. Then switch to incremental mode after the table scan snapshot.
   */
  TABLE_SCAN_THEN_INCREMENTAL,

  /**
   * Start incremental mode from the latest snapshot inclusive
   *
   * If it is an empty map, all future append snapshots should be discovered
   */
  INCREMENTAL_FROM_LATEST_SNAPSHOT,

  /**
   * Start incremental mode from the earliest snapshot inclusive
   *
   * If it is an empty map, all future append snapshots should be discovered.
   */
  INCREMENTAL_FROM_EARLIEST_SNAPSHOT,

  /**
   * Start incremental mode from a snapshot with a specific id inclusive
   */
  INCREMENTAL_FROM_SNAPSHOT_ID,

  /**
   * Start incremental mode from a snapshot with a specific timestamp inclusive
   *
   * Starting snapshot has a timestamp lower than or equal to the specified timestamp
   */
  INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP
}
