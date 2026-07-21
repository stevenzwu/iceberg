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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
 * Measures the per-manifest-entry overhead of preserving the legacy {@code null}-when-empty
 * contract on the read path when presenting {@link ContentStats} as the five legacy stat maps.
 *
 * <p>The variants build the five {@link ContentStatsBackedMap} views a read-direction adapter
 * returns for one entry. {@code emptyMap} returns each view directly (a present-but-empty view is a
 * non-null empty map). {@code nullWhenEmpty} runs a hand-inlined, allocation-free emptiness scan
 * per view and returns {@code null} when empty, matching what the eager converters returned. {@code
 * nullViaFactory} does the same through the production {@link ContentStatsBackedMap#forKind}
 * factory, so it should track {@code nullWhenEmpty}. {@code nullViaEntrySet} instead materializes
 * each view's {@code entrySet()} to decide emptiness — the naive check the {@code isEmpty()}
 * override and {@code forKind} avoid. Columns are {@code optional long}, so value/null counts and
 * bounds are present (their scan short-circuits on the first column) while {@code nan_value_count}
 * is tracked by no column — the realistic case for a table with no floating-point columns, where
 * the emptiness scan walks every column.
 *
 * <p>Run: {@code ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ContentStatsMapNullabilityBenchmark}
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ContentStatsMapNullabilityBenchmark {

  @Param({"2", "10", "50", "200"})
  private int numColumns;

  private ContentStats stats;

  private static final ContentStatsBackedMap.Kind[] KINDS = ContentStatsBackedMap.Kind.values();

  @Setup(Level.Trial)
  public void setup() {
    List<Types.NestedField> fields = Lists.newArrayListWithCapacity(numColumns);
    for (int i = 0; i < numColumns; i++) {
      fields.add(Types.NestedField.optional(i + 1, "c" + i, Types.LongType.get()));
    }

    Schema schema = new Schema(fields);
    List<Integer> ids = Lists.newArrayListWithCapacity(numColumns);
    for (Types.NestedField field : schema.columns()) {
      ids.add(field.fieldId());
    }

    Types.StructType statsType = StatsUtil.statsReadSchema(schema, ids);
    ContentStatsStruct built = new ContentStatsStruct(statsType);
    for (int id = 1; id <= numColumns; id++) {
      Types.StructType fieldType = statsType.field(StatsUtil.toBaseId(id)).type().asStructType();
      built.setStats(
          id,
          new FieldStatsStruct<>(
              fieldType,
              (long) id,
              (long) (id + 1_000),
              false,
              1_000L + id,
              (long) id,
              null,
              null));
    }

    this.stats = built;
  }

  /** Shipped behavior: return each lazy view directly; a present-but-empty view is an empty map. */
  @Benchmark
  public void emptyMap(Blackhole blackhole) {
    for (ContentStatsBackedMap.Kind kind : KINDS) {
      blackhole.consume(new ContentStatsBackedMap<>(stats, kind));
    }
  }

  /** Preserve null-when-empty: pay an allocation-free emptiness scan per view to decide null. */
  @Benchmark
  public void nullWhenEmpty(Blackhole blackhole) {
    for (ContentStatsBackedMap.Kind kind : KINDS) {
      Map<Integer, ?> view = new ContentStatsBackedMap<>(stats, kind);
      blackhole.consume(isEmptyView(kind) ? null : view);
    }
  }

  /** Preserve null-when-empty through the production factory: forKind runs the emptiness scan. */
  @Benchmark
  public void nullViaFactory(Blackhole blackhole) {
    for (ContentStatsBackedMap.Kind kind : KINDS) {
      blackhole.consume(ContentStatsBackedMap.forKind(kind, stats));
    }
  }

  /** Naive null-when-empty: materialize entrySet() to decide emptiness (the pre-override cost). */
  @Benchmark
  public void nullViaEntrySet(Blackhole blackhole) {
    for (ContentStatsBackedMap.Kind kind : KINDS) {
      Map<Integer, ?> view = new ContentStatsBackedMap<>(stats, kind);
      Set<?> entries = view.entrySet();
      blackhole.consume(entries);
      blackhole.consume(entries.isEmpty() ? null : view);
    }
  }

  // Allocation-free (aside from the fieldStats iterator) emptiness probe: the proposed isEmpty()
  // override. Short-circuits on the first column that contributes an entry for the kind.
  private boolean isEmptyView(ContentStatsBackedMap.Kind kind) {
    for (FieldStats<?> fs : stats.fieldStats()) {
      if (contributes(fs, kind)) {
        return false;
      }
    }

    return true;
  }

  private boolean contributes(FieldStats<?> fs, ContentStatsBackedMap.Kind kind) {
    switch (kind) {
      case VALUE_COUNT:
        return true;
      case NULL_VALUE_COUNT:
        return fs.hasNullCount();
      case NAN_VALUE_COUNT:
        return fs.hasNaNCount();
      case LOWER_BOUND:
        return fs.lowerBound() != null && fs.type().fieldType("lower_bound") != null;
      case UPPER_BOUND:
        return fs.upperBound() != null && fs.type().fieldType("upper_bound") != null;
      default:
        throw new IllegalArgumentException("Unknown content stats kind: " + kind);
    }
  }
}
