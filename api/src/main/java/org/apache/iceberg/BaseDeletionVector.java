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

import java.util.Objects;

class BaseDeletionVector implements DeletionVector {
  private final String location;
  private final long offset;
  private final long sizeInBytes;
  private final long cardinality;

  BaseDeletionVector(String location, long offset, long sizeInBytes, long cardinality) {
    if (location == null) {
      throw new IllegalArgumentException("Invalid location: null");
    }
    if (offset < 0) {
      throw new IllegalArgumentException("Invalid offset: " + offset + " (must be >= 0)");
    }
    if (sizeInBytes < 0) {
      throw new IllegalArgumentException(
          "Invalid size in bytes: " + sizeInBytes + " (must be >= 0)");
    }
    if (cardinality < 0) {
      throw new IllegalArgumentException("Invalid cardinality: " + cardinality + " (must be >= 0)");
    }
    this.location = location;
    this.offset = offset;
    this.sizeInBytes = sizeInBytes;
    this.cardinality = cardinality;
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public long offset() {
    return offset;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public long cardinality() {
    return cardinality;
  }

  @Override
  public DeletionVector copy() {
    return new BaseDeletionVector(location, offset, sizeInBytes, cardinality);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof DeletionVector)) {
      return false;
    }

    DeletionVector that = (DeletionVector) other;
    return Objects.equals(location, that.location())
        && offset == that.offset()
        && sizeInBytes == that.sizeInBytes()
        && cardinality == that.cardinality();
  }

  @Override
  public int hashCode() {
    return Objects.hash(location, offset, sizeInBytes, cardinality);
  }

  @Override
  public String toString() {
    return "DeletionVector{location="
        + location
        + ", offset="
        + offset
        + ", size_in_bytes="
        + sizeInBytes
        + ", cardinality="
        + cardinality
        + "}";
  }
}
