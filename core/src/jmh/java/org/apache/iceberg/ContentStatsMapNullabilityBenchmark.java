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

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Measures the per-manifest-entry cost of presenting a v4 {@link ContentStats} as the legacy
 * per-column {@link ContentFile} stat maps. {@code emptyMap} builds the five {@link
 * ContentStatsBackedMap} views directly (a present-but-empty view stays a non-null empty map);
 * {@code nullViaFactory} builds them through {@link ContentStatsBackedMap#forKind}, which returns
 * {@code null} for a metric no column tracks. Columns rotate the same eight types as {@link
 * ContentStatsReadBenchmark}; because float and double columns track {@code nan_value_count}, its
 * emptiness scan short-circuits on the first floating-point column like the other four metrics,
 * rather than walking every column as an all-{@code long} schema would force.
 *
 * <p>Run: {@code ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ContentStatsMapNullabilityBenchmark}
 */
@Fork(5)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ContentStatsMapNullabilityBenchmark {

  // rotate the same eight column types as ContentStatsReadBenchmark so float/double columns track
  // nan_value_count and its emptiness scan short-circuits rather than walking every column
  private static final Type[] COLUMN_TYPES = {
    Types.BooleanType.get(),
    Types.IntegerType.get(),
    Types.LongType.get(),
    Types.FloatType.get(),
    Types.DoubleType.get(),
    Types.StringType.get(),
    Types.DateType.get(),
    Types.TimestampType.withoutZone(),
  };

  @Param({"2", "10", "50", "200"})
  private int numColumns;

  private ContentStats stats;

  private static final ContentStatsBackedMap.Kind[] KINDS = ContentStatsBackedMap.Kind.values();

  @Setup(Level.Trial)
  public void setupBenchmark() {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(numColumns);
    for (int i = 0; i < numColumns; i++) {
      fields.add(Types.NestedField.optional(i + 1, "c" + i, COLUMN_TYPES[i % COLUMN_TYPES.length]));
    }

    Schema schema = new Schema(fields);
    List<Integer> ids = Lists.newArrayListWithCapacity(numColumns);
    for (Types.NestedField field : schema.columns()) {
      ids.add(field.fieldId());
    }

    Types.StructType statsType = StatsUtil.statsReadSchema(schema, ids);
    ContentStatsStruct built = new ContentStatsStruct(statsType);
    for (int id = 1; id <= numColumns; id++) {
      Type type = schema.findType(id);
      Types.StructType fieldType = statsType.field(StatsUtil.toBaseId(id)).type().asStructType();
      boolean floating = type.typeId() == Type.TypeID.FLOAT || type.typeId() == Type.TypeID.DOUBLE;
      Long nanValueCount = floating ? 0L : null;
      built.setStats(
          id,
          new FieldStatsStruct<>(
              fieldType,
              lowerBoundFor(type, id),
              upperBoundFor(type, id),
              false,
              1_000L + id,
              (long) id,
              nanValueCount,
              null));
    }

    this.stats = built;
  }

  private static Object lowerBoundFor(Type type, int id) {
    switch (type.typeId()) {
      case BOOLEAN:
        return false;
      case INTEGER:
      case DATE:
        return id;
      case LONG:
      case TIMESTAMP:
        return (long) id;
      case FLOAT:
        return (float) id;
      case DOUBLE:
        return (double) id;
      case STRING:
        return "lo" + id;
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  private static Object upperBoundFor(Type type, int id) {
    switch (type.typeId()) {
      case BOOLEAN:
        return true;
      case INTEGER:
      case DATE:
        return id + 1_000;
      case LONG:
      case TIMESTAMP:
        return (long) (id + 1_000);
      case FLOAT:
        return (float) (id + 1_000);
      case DOUBLE:
        return (double) (id + 1_000);
      case STRING:
        return "up" + id;
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  /** Shipped behavior: return each lazy view directly; a present-but-empty view is an empty map. */
  @Benchmark
  public void emptyMap(Blackhole blackhole) {
    for (ContentStatsBackedMap.Kind kind : KINDS) {
      blackhole.consume(new ContentStatsBackedMap<>(stats, kind));
    }
  }

  /** Preserve null-when-empty through the production factory: forKind runs the emptiness scan. */
  @Benchmark
  public void nullViaFactory(Blackhole blackhole) {
    for (ContentStatsBackedMap.Kind kind : KINDS) {
      blackhole.consume(ContentStatsBackedMap.forKind(kind, stats));
    }
  }
}
