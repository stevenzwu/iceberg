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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED;
import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.io.IOException;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.metrics.CommitMetrics;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ImmutableCommitReport;
import org.apache.iceberg.metrics.LoggingMetricsReporter;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.Timer.Timed;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.math.IntMath;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Exceptions;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps common functionality to create a new snapshot.
 *
 * <p>The number of attempted commits is controlled by {@link TableProperties#COMMIT_NUM_RETRIES}
 * and {@link TableProperties#COMMIT_NUM_RETRIES_DEFAULT} properties.
 */
@SuppressWarnings("UnnecessaryAnonymousClass")
abstract class SnapshotProducer<ThisT> implements SnapshotUpdate<ThisT> {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotProducer.class);
  static final int MIN_FILE_GROUP_SIZE = 10_000;
  static final Set<ManifestFile> EMPTY_SET = Sets.newHashSet();

  /**
   * Fallback seed for {@link V4CommitAccumulator}'s {@code avgBytesPerEntry} when a prior leaf's
   * on-disk size is not available for sampling. Chosen to keep small commits under the target
   * (staying inline in the root manifest) while still triggering a spill for realistically-sized
   * appends. A later slice replaces this with a per-commit sample from the most recent prior leaf.
   */
  private static final double DEFAULT_AVG_BYTES_PER_ENTRY_SEED = 200.0d;

  /** Default callback used to delete files. */
  private final Consumer<String> defaultDelete =
      new Consumer<String>() {
        @Override
        public void accept(String file) {
          ops.io().deleteFile(file);
        }
      };

  /** Cache used to enrich ManifestFile instances that are written to a ManifestListWriter. */
  private final LoadingCache<ManifestFile, ManifestFile> manifestsWithMetadata;

  private final TableOperations ops;
  private final boolean strictCleanup;
  private final boolean canInheritSnapshotId;
  private final String commitUUID = UUID.randomUUID().toString();
  private final AtomicInteger manifestCount = new AtomicInteger(0);
  private final AtomicInteger attempt = new AtomicInteger(0);
  private final List<String> manifestLists = Lists.newArrayList();
  private final long targetManifestSizeBytes;
  private final List<TrackedFile> v4AdaptiveNewLiveDataRows = Lists.newArrayList();
  private final List<TrackedFile> v4AdaptiveRetirementRows = Lists.newArrayList();
  private final FileFormat manifestFormat;
  private final Map<String, String> manifestWriterProps;

  private long v4DrainedDirectRowRecords = 0L;
  private MetricsReporter reporter = LoggingMetricsReporter.instance();
  private volatile Long snapshotId = null;
  private TableMetadata base;
  private boolean stageOnly = false;
  private Consumer<String> deleteFunc = defaultDelete;
  private SnapshotAncestryValidator snapshotAncestryValidator =
      SnapshotAncestryValidator.NON_VALIDATING;

  private ExecutorService workerPool;
  private ExecutorService writePool;
  private int writePoolParallelism = ThreadPools.WORKER_THREAD_POOL_SIZE;
  private String targetBranch = SnapshotRef.MAIN_BRANCH;
  private CommitMetrics commitMetrics;

  protected SnapshotProducer(TableOperations ops) {
    this.ops = ops;
    this.strictCleanup = ops.requireStrictCleanup();
    this.base = ops.current();
    this.manifestsWithMetadata =
        Caffeine.newBuilder()
            .build(
                file -> {
                  if (file.snapshotId() != null) {
                    return file;
                  }
                  return addMetadata(ops, file);
                });
    this.targetManifestSizeBytes =
        ops.current()
            .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
    this.manifestFormat =
        ops.current().formatVersion() >= TableMetadata.MIN_FORMAT_VERSION_PARQUET_MANIFESTS
            ? FileFormat.PARQUET
            : FileFormat.AVRO;
    this.manifestWriterProps = manifestWriterProperties(ops.current());
    boolean snapshotIdInheritanceEnabled =
        ops.current()
            .propertyAsBoolean(
                SNAPSHOT_ID_INHERITANCE_ENABLED, SNAPSHOT_ID_INHERITANCE_ENABLED_DEFAULT);
    this.canInheritSnapshotId = ops.current().formatVersion() > 1 || snapshotIdInheritanceEnabled;
  }

  protected abstract ThisT self();

  @Override
  public ThisT stageOnly() {
    this.stageOnly = true;
    return self();
  }

  @Override
  public ThisT scanManifestsWith(ExecutorService executorService) {
    this.workerPool = executorService;
    return self();
  }

  @Override
  public ThisT writeManifestsWith(ExecutorService executorService, int parallelism) {
    Preconditions.checkArgument(executorService != null, "Executor service cannot be null");
    Preconditions.checkArgument(
        parallelism > 0, "Parallelism must be greater than 0, but was: %s", parallelism);
    this.writePool = executorService;
    this.writePoolParallelism = parallelism;
    return self();
  }

  /**
   * Set a validator to check snapshot ancestry before committing changes.
   *
   * <p>If there is no parent snapshot, an empty iterable will be supplied to the validator.
   *
   * @param validator a validator to check snapshot ancestry validity
   * @return this for method chaining
   */
  @Override
  public ThisT validateWith(SnapshotAncestryValidator validator) {
    this.snapshotAncestryValidator = validator;
    return self();
  }

  protected TableOperations ops() {
    return ops;
  }

  protected CommitMetrics commitMetrics() {
    if (commitMetrics == null) {
      this.commitMetrics = CommitMetrics.of(new DefaultMetricsContext());
    }

    return commitMetrics;
  }

  protected ThisT reportWith(MetricsReporter newReporter) {
    this.reporter = newReporter;
    return self();
  }

  /**
   * A setter for the target branch on which snapshot producer operation should be performed
   *
   * @param branch to set as target branch
   */
  protected void targetBranch(String branch) {
    Preconditions.checkArgument(branch != null, "Invalid branch name: null");
    boolean refExists = base.ref(branch) != null;
    Preconditions.checkArgument(
        !refExists || base.ref(branch).isBranch(),
        "%s is a tag, not a branch. Tags cannot be targets for producing snapshots",
        branch);
    this.targetBranch = branch;
  }

  protected String targetBranch() {
    return targetBranch;
  }

  protected ExecutorService workerPool() {
    if (workerPool == null) {
      this.workerPool = ThreadPools.getWorkerPool();
    }

    return workerPool;
  }

  protected ExecutorService writePool() {
    if (writePool == null) {
      this.writePool = ThreadPools.getWorkerPool();
    }

    return writePool;
  }

  @Override
  public ThisT deleteWith(Consumer<String> deleteCallback) {
    Preconditions.checkArgument(
        this.deleteFunc == defaultDelete, "Cannot set delete callback more than once");
    this.deleteFunc = deleteCallback;
    return self();
  }

  /**
   * Clean up any uncommitted manifests that were created.
   *
   * <p>Manifests may not be committed if apply is called more because a commit conflict has
   * occurred. Implementations may keep around manifests because the same changes will be made by
   * both apply calls. This method instructs the implementation to clean up those manifests and
   * passes the paths of the manifests that were actually committed.
   *
   * @param committed a set of manifest paths that were actually committed
   */
  protected abstract void cleanUncommitted(Set<ManifestFile> committed);

  /**
   * A string that describes the action that produced the new snapshot.
   *
   * @return a string operation
   */
  protected abstract String operation();

  /**
   * Validate the current metadata.
   *
   * <p>Child operations can override this to add custom validation.
   *
   * @param currentMetadata current table metadata to validate
   * @param snapshot ending snapshot on the lineage which is being validated
   */
  protected void validate(TableMetadata currentMetadata, Snapshot snapshot) {}

  /**
   * Applies the update's changes and returns the manifests the new snapshot records at its top
   * level.
   *
   * <p>For v1–v3 this is the complete manifest list of the new snapshot: this commit's newly
   * written data manifests plus the filtered and merged parent and delete manifests. For v4+ it is
   * only the pre-built leaf manifests recorded in the root as leaf-manifest entries
   * (caller-appended manifests, deletion-vector-rewritten leaves, and surviving parent leaves);
   * this commit's new, retired, and DV-updated row content is instead staged into the adaptive-tree
   * accumulator channels and drained into the root by {@link #applyRootManifest}.
   *
   * @param metadataToUpdate the base table metadata to apply changes to
   * @param snapshot the parent snapshot the changes apply to
   * @return the manifests recorded at the new snapshot's top level (see above)
   */
  protected abstract List<ManifestFile> apply(TableMetadata metadataToUpdate, Snapshot snapshot);

  @Override
  public Snapshot apply() {
    refresh();
    Snapshot parentSnapshot = SnapshotUtil.latestSnapshot(base, targetBranch);

    long sequenceNumber = base.nextSequenceNumber();

    runValidations(parentSnapshot);

    int formatVersion = base.formatVersion();

    if (formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE) {
      return applyRootManifest(parentSnapshot, sequenceNumber, formatVersion);
    } else {
      return applyManifestList(parentSnapshot, sequenceNumber, formatVersion);
    }
  }

  private Snapshot applyManifestList(
      Snapshot parentSnapshot, long sequenceNumber, int formatVersion) {
    Long parentSnapshotId = parentSnapshot == null ? null : parentSnapshot.snapshotId();
    List<ManifestFile> manifests = apply(base, parentSnapshot);
    OutputFile manifestList = manifestListPath();

    ManifestListWriter writer =
        ManifestLists.write(
            formatVersion,
            manifestList,
            ops.encryption(),
            snapshotId(),
            parentSnapshotId,
            sequenceNumber,
            base.nextRowId());

    try (writer) {
      // keep track of the manifest lists created
      manifestLists.add(manifestList.location());

      ManifestFile[] manifestFiles = new ManifestFile[manifests.size()];

      Tasks.range(manifestFiles.length)
          .stopOnFailure()
          .throwFailureWhenFinished()
          .executeWith(workerPool())
          .run(index -> manifestFiles[index] = manifestsWithMetadata.get(manifests.get(index)));

      writer.addAll(Arrays.asList(manifestFiles));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest list file");
    }

    Long nextRowId = null;
    Long assignedRows = null;
    if (formatVersion >= 3) {
      nextRowId = base.nextRowId();
      assignedRows = writer.nextRowId() - base.nextRowId();
    }

    Map<String, String> summary = summary();
    String operation = operation();

    if (summary != null && DataOperations.REPLACE.equals(operation)) {
      long addedRecords =
          PropertyUtil.propertyAsLong(summary, SnapshotSummary.ADDED_RECORDS_PROP, 0L);
      long replacedRecords =
          PropertyUtil.propertyAsLong(summary, SnapshotSummary.DELETED_RECORDS_PROP, 0L);

      // added may be less than replaced when records are already deleted by delete files
      Preconditions.checkArgument(
          addedRecords <= replacedRecords,
          "Invalid REPLACE operation: %s added records > %s replaced records",
          addedRecords,
          replacedRecords);
    }

    return new BaseSnapshot(
        sequenceNumber,
        snapshotId(),
        parentSnapshotId,
        System.currentTimeMillis(),
        operation(),
        summary(base),
        base.currentSchemaId(),
        manifestList.location(),
        nextRowId,
        assignedRows,
        writer.toManifestListFile().encryptionKeyID());
  }

  private Snapshot applyRootManifest(
      Snapshot parentSnapshot, long sequenceNumber, int formatVersion) {
    Long parentSnapshotId = parentSnapshot == null ? null : parentSnapshot.snapshotId();
    List<ManifestFile> preBuiltLeafManifests = apply(base, parentSnapshot);

    // No-op detection: if this commit's manifest set is identical to the parent's (same paths)
    // AND no direct-row content is queued in the accumulator input channels, none of the
    // manifests were written by this snapshot and we can reuse the parent's root manifest.
    // Writing a new root manifest in that case leaves an orphan file that the cleanup path
    // deletes, and which would be referenced by the new snapshot's snapshotFileLocation if the
    // commit went through unchanged. Adaptive small-write commits inject rows directly into the
    // accumulator (never surfaced as ManifestFile entries) — skipping the no-op branch when the
    // channels are non-empty ensures those rows drain into a fresh promoted root.
    if (parentSnapshot != null
        && parentSnapshot.snapshotFileLocation() != null
        && v4AdaptiveNewLiveDataRows.isEmpty()
        && v4AdaptiveRetirementRows.isEmpty()
        && isNoOp(
            preBuiltLeafManifests,
            filterVirtualDirectRows(parentSnapshot.allManifests(ops.io())))) {
      return new BaseSnapshot(
          formatVersion,
          sequenceNumber,
          snapshotId(),
          parentSnapshotId,
          System.currentTimeMillis(),
          operation(),
          summary(base),
          base.currentSchemaId(),
          parentSnapshot.snapshotFileLocation(),
          base.nextRowId(),
          0L,
          parentSnapshot.keyId());
    }

    // Enrich manifest metadata in parallel (same pattern as v3).
    ManifestFile[] manifestFiles = new ManifestFile[preBuiltLeafManifests.size()];
    Tasks.range(manifestFiles.length)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(workerPool())
        .run(
            index ->
                manifestFiles[index] = manifestsWithMetadata.get(preBuiltLeafManifests.get(index)));

    long currentSnapshotId = snapshotId();

    // Adaptive path is the only v4 write path: the accumulator streams live-pool rows through a
    // StreamingLeafManifestWriter, promotes the last-open writer to root with all
    // leaf-manifest-entries appended. Small commits stay inline as root direct rows; larger commits
    // spill leaves.
    SnapshotFile promotedRoot =
        runAdaptiveDrainAndPromote(
            manifestFiles, parentSnapshot, currentSnapshotId, sequenceNumber);
    String snapshotFileLocation = promotedRoot.location();
    // Null for promoted-leaf roots (their appender carries no snapshot file encryption key —
    // future work); the value flows through the SnapshotFile from
    // RootManifestWriter#toSnapshotFile.
    String snapshotFileEncryptionKeyId = promotedRoot.encryptionKeyID();
    manifestLists.add(snapshotFileLocation);

    Map<String, String> summary = summary();
    String operation = operation();

    if (summary != null && DataOperations.REPLACE.equals(operation)) {
      long addedRecords =
          PropertyUtil.propertyAsLong(summary, SnapshotSummary.ADDED_RECORDS_PROP, 0L);
      long replacedRecords =
          PropertyUtil.propertyAsLong(summary, SnapshotSummary.DELETED_RECORDS_PROP, 0L);

      // added may be less than replaced when records are already deleted by delete files
      Preconditions.checkArgument(
          addedRecords <= replacedRecords,
          "Invalid REPLACE operation: %s added records > %s replaced records",
          addedRecords,
          replacedRecords);
    }

    // v4+ snapshots must carry first-row-id and added-rows for row lineage tracking. Newly-added
    // rows arrive either as ADDED entries in a manifest (counted by computeAssignedRows) or as
    // rows streamed through the accumulator's live pool (tracked by v4DrainedDirectRowRecords in
    // runAdaptiveDrainAndPromote; covers both promoted-root direct rows and rolled-leaf rows).
    Long firstRowId = base.nextRowId();
    Long addedRows = computeAssignedRows(manifestFiles) + v4DrainedDirectRowRecords;

    return new BaseSnapshot(
        formatVersion,
        sequenceNumber,
        snapshotId(),
        parentSnapshotId,
        System.currentTimeMillis(),
        operation(),
        summary(base),
        base.currentSchemaId(),
        snapshotFileLocation,
        firstRowId,
        addedRows,
        snapshotFileEncryptionKeyId);
  }

  /**
   * Drains the v4 adaptive-tree live-data channel through a {@link V4CommitAccumulator} and
   * promotes the accumulator's last streamed writer to the snapshot's root manifest. External
   * leaves (from {@code manifestFiles}) are added as manifest-reference rows in the promoted root
   * alongside the accumulator's own rolled leaves.
   *
   * <p>The v4 leaves the accumulator produces (via {@link StreamingLeafManifestWriter}) share the
   * table-wide union partition type ({@link Partitioning#unionPartitionTypes}); each row's
   * partition tuple projects into the union at wrap time. The current default spec is passed to the
   * leaf writer factory because {@link LeafManifestWriter#forData} only uses it for schema
   * derivation and specId on the leaf's manifest header — the on-disk partition type is the union.
   *
   * <p>Every live-pool row's record count contributes to {@link #v4DrainedDirectRowRecords}, which
   * applyRootManifest folds into the snapshot's added-rows total. The counter covers both rows that
   * end up as promoted-root direct rows and rows that ended up in accumulator-rolled leaves (the
   * rolled leaves are not in {@code manifestFiles}, so {@link #computeAssignedRows} would otherwise
   * miss them).
   *
   * <p>Retirement rows unpacked from source manifests filtered by {@code ManifestFilterManager} are
   * drained here from {@link #v4AdaptiveRetirementRows}: DELETED rows land in the deleted
   * retirement pool, EXISTING survivors land in the live pool. Their tails flow into the promoted
   * root as direct rows and spilled leaves flow in as leaf-manifest-entry refs. Eq-delete pools are
   * still Phase 6 work.
   */
  private SnapshotFile runAdaptiveDrainAndPromote(
      ManifestFile[] externalManifests,
      Snapshot parentSnapshot,
      long currentSnapshotId,
      long sequenceNumber) {
    TableMetadata current = ops.current();
    Types.StructType unionPartitionType =
        Partitioning.unionPartitionTypes(current.specsById().values());
    PartitionSpec defaultSpec = current.spec();
    long snapId = snapshotId();
    Supplier<LeafManifestWriter> leafWriterFactory =
        () ->
            LeafManifestWriter.forData(
                defaultSpec,
                unionPartitionType,
                newManifestOutputFile(),
                snapId,
                null,
                manifestWriterProps);

    V4CommitAccumulator accumulator =
        new V4CommitAccumulator(
            leafWriterFactory, targetManifestSizeBytes, DEFAULT_AVG_BYTES_PER_ENTRY_SEED);

    // Phase 4c: carry the parent snapshot's direct data rows into the child's promoted root as
    // EXISTING live rows. Feeding them through the live pool (instead of force-appending them as
    // direct rows) lets them roll into leaves uniformly when the combined live set is large;
    // without
    // carrying them at all, small-write adaptive commits chain into data loss — the parent's direct
    // rows are referenced from no leaf manifest, so snapshot.allManifests() misses them. They are
    // EXISTING, so they do not contribute to v4DrainedDirectRowRecords.
    for (TrackedFile row : readParentDirectRowsAsExisting(parentSnapshot)) {
      accumulator.add(row);
    }

    for (TrackedFile row : v4AdaptiveNewLiveDataRows) {
      accumulator.add(row);
      // Freshly-added rows carry newly-assigned first-row-ids that no external ManifestFile's
      // addedRowsCount surfaces to computeAssignedRows; the counter covers both rows destined for
      // the promoted-root as direct rows and rows destined for accumulator-rolled leaves.
      if (row.tracking().status() == EntryStatus.ADDED && row.tracking().firstRowId() == null) {
        this.v4DrainedDirectRowRecords += row.recordCount();
      }
    }

    // Retirement rows (unpacked from source manifests being filtered): EXISTING survivors flow into
    // the live pool, DELETED retirements flow into the deleted-retirement pool. The accumulator
    // routes by row status. Retirement rows do not add to v4DrainedDirectRowRecords: their
    // firstRowId was already assigned in the prior snapshot that produced them, so they are not
    // "newly-added" rows for this commit.
    for (TrackedFile row : v4AdaptiveRetirementRows) {
      accumulator.add(row);
    }

    for (ManifestFile external : externalManifests) {
      EntryStatus status =
          external.snapshotId() != null && external.snapshotId() == currentSnapshotId
              ? EntryStatus.ADDED
              : EntryStatus.EXISTING;
      accumulator.addExternalLeafManifestEntry(external, status);
    }

    return accumulator.close(snapId, sequenceNumber, base.nextRowId());
  }

  /**
   * Reads {@code parentSnapshot}'s direct data rows from its root manifest and rewrites each row's
   * tracking status to {@link EntryStatus#EXISTING} (preserving snapshot-id, sequence numbers,
   * first-row-id, and DV snapshot-id) so the child snapshot's promoted root carries them forward as
   * inline references to the same underlying data files.
   *
   * <p>Returns empty for a null parent, a parent that pre-dates v4, or a parent whose root has no
   * direct rows (a pure-reference parent). Costs one Parquet read of the parent's root manifest per
   * commit; a future slice can cache this on the parent Snapshot.
   */
  private List<TrackedFile> readParentDirectRowsAsExisting(Snapshot parentSnapshot) {
    if (parentSnapshot == null
        || parentSnapshot.formatVersion()
            < TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE) {
      return ImmutableList.of();
    }
    TableMetadata current = ops.current();
    Map<Integer, PartitionSpec> specsById = current.specsById();
    List<TrackedFile> raw =
        RootManifestReader.readDirectRows(
            ops.io().newInputFile(parentSnapshot.snapshotFileLocation()), specsById);
    if (raw.isEmpty()) {
      return ImmutableList.of();
    }

    Types.StructType unionPartitionType = Partitioning.unionPartitionTypes(specsById.values());
    MetricsConfig metricsConfig = MetricsConfig.from(current.properties(), current.schema(), null);

    // Phase 4b: filter-manager may have already routed a subset of parent's direct rows into the
    // retirement channel as DELETED. Skip those here so they are not also carried into the child's
    // promoted root as EXISTING — that would resurrect files the current commit is retiring.
    Set<String> retiredPaths = Sets.newHashSet();
    for (TrackedFile r : v4AdaptiveRetirementRows) {
      retiredPaths.add(r.location());
    }

    // Direct rows written as ADDED with null dataSequenceNumber (FastAppend's inject-inherit
    // pattern) come back from disk with null seq/fileSeq. Resolve those against the parent's
    // snapshot sequence number when rewriting to EXISTING — the parent's seq is what the reader
    // would have inherited from the commit that produced these rows.
    long parentSeq = parentSnapshot.sequenceNumber();
    List<TrackedFile> carried = Lists.newArrayListWithCapacity(raw.size());
    for (TrackedFile row : raw) {
      if (retiredPaths.contains(row.location())) {
        continue;
      }
      DataFile file = TrackedFileAdapters.asDataFile(row, specsById);
      Tracking src = row.tracking();
      long resolvedDataSeq =
          src.dataSequenceNumber() != null ? src.dataSequenceNumber() : parentSeq;
      Long resolvedFileSeq =
          src.fileSequenceNumber() != null ? src.fileSequenceNumber() : parentSeq;
      Tracking newTracking =
          new TrackingStruct(
              EntryStatus.EXISTING,
              src.snapshotId(),
              resolvedDataSeq,
              resolvedFileSeq,
              src.dvSnapshotId(),
              src.firstRowId(),
              null,
              null);
      // Fresh adapter per row so the buffered TrackedFile retains distinct state (the accumulator
      // holds row references in memory; a reusable wrapper shared across rows corrupts the list).
      TrackedFileAdapters.DataTrackedFile adapter =
          TrackedFileAdapters.forDataFile(
              TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
              current.schema(),
              metricsConfig,
              unionPartitionType);
      carried.add(adapter.wrap(file, newTracking));
    }
    return carried;
  }

  /**
   * Appends a {@link TrackedFile} row to the v4 adaptive-tree live-data channel. Called by
   * v4-adaptive producer subclasses that route new content files directly into the accumulator
   * instead of writing leaf manifests up front; {@link #runAdaptiveDrainAndPromote} drains this
   * buffer when the adaptive-tree flag is on. Every row lands in the same non-per-spec pool — its
   * partition tuple must already be projected into the table-wide union partition type by the
   * wrapper.
   */
  void addV4AdaptiveNewLiveDataRow(TrackedFile row) {
    Preconditions.checkArgument(row != null, "Invalid TrackedFile row: null");
    v4AdaptiveNewLiveDataRows.add(row);
  }

  /**
   * Appends a {@link TrackedFile} row unpacked from a source manifest that is being filtered out of
   * the snapshot's leaf set into the v4 adaptive-tree retirement channel. The row's status
   * (EXISTING for a survivor, DELETED for a retirement) drives which pool the accumulator routes it
   * to. Callers include {@code ManifestFilterManager} on v4 tables, where a partially-filtered or
   * all-deleted source manifest's rows are routed here instead of being copied into a rewritten
   * leaf manifest.
   */
  void addV4AdaptiveRetirementRow(TrackedFile row) {
    Preconditions.checkArgument(row != null, "Invalid TrackedFile row: null");
    v4AdaptiveRetirementRows.add(row);
  }

  /**
   * Discards any previously-buffered adaptive rows and resets the drained-record counter. Called by
   * v4-adaptive producer subclasses at the top of each {@code apply()} attempt so a retry after a
   * commit conflict starts from a clean buffer instead of accumulating stale rows from the prior
   * attempt.
   */
  void clearV4AdaptiveInputs() {
    v4AdaptiveNewLiveDataRows.clear();
    v4AdaptiveRetirementRows.clear();
    this.v4DrainedDirectRowRecords = 0L;
  }

  /**
   * Discards only the previously-buffered new-live rows and resets the drained-record counter,
   * leaving any retirement rows populated earlier in this {@code apply()} attempt untouched. Called
   * by {@link #injectV4AdaptiveDataFiles} so it can be invoked after {@code ManifestFilterManager}
   * has already routed retirement rows into the accumulator's retirement buffer within the same
   * attempt.
   */
  private void clearV4AdaptiveNewLiveInputs() {
    v4AdaptiveNewLiveDataRows.clear();
    this.v4DrainedDirectRowRecords = 0L;
  }

  /**
   * Discards previously-buffered retirement rows so a subsequent {@code apply()} attempt starts
   * from a clean retirement channel. Called by v4-adaptive producer subclasses at the top of each
   * attempt (before {@code ManifestFilterManager.filterManifests} runs) to guard against stale
   * retirement rows accumulating across commit retries.
   */
  void clearV4AdaptiveRetirementInputs() {
    v4AdaptiveRetirementRows.clear();
  }

  /**
   * Converts the given {@link DataFile}s into {@link TrackedFile} rows and routes them into the v4
   * adaptive-tree live-data channel. Discards any live rows from a prior {@code apply()} attempt
   * (retaining retirement rows populated earlier in this attempt), then wraps each file through a
   * fresh {@link TrackedFileAdapters.DataTrackedFile} (buffered rows must retain distinct state — a
   * reusable wrapper shared across rows corrupts the buffer). Rows from any spec share one
   * accumulator pool: every wrapper uses the table-wide union partition type so per-file partitions
   * project uniformly, and the {@link MetricsConfig}-derived stats shape matches the root writer's
   * schema chosen in {@link #applyRootManifest}.
   *
   * <p>When {@code bornWithDVByPath} contains a DV for a file's path, the row is wrapped via the
   * 3-arg {@link TrackedFileAdapters.DataTrackedFile#wrap(DataFile, Tracking, DeletionVector)}
   * variant so the DV column is populated on the accumulator side, and the row's {@code
   * tracking.dvSnapshotId} is stamped with the current commit's snapshot id.
   *
   * @param files new data files across all specs; callers concatenate their spec-keyed maps via
   *     {@code Iterables.concat(bySpec.values())}
   * @param explicitDataSequenceNumber overrides the row's data sequence number when non-null; when
   *     null the row inherits the commit's sequence number at read time (v3-style inheritance)
   * @param bornWithDVByPath map from data file path to a DV {@link DeleteFile} for files whose DV
   *     was written in the same commit (RowDelta.addRows). Empty or null when no DVs are attached.
   */
  protected void injectV4AdaptiveDataFiles(
      Iterable<DataFile> files,
      Long explicitDataSequenceNumber,
      Map<String, DeleteFile> bornWithDVByPath) {
    clearV4AdaptiveNewLiveInputs();
    TableMetadata current = ops.current();
    // The adapter's partition projection must use the same union type the leaf writers use, so the
    // written row and the on-disk schema agree. An empty union (unpartitioned) maps to UnknownType
    // via TrackedFile.schema — the partition column is omitted and read back as null.
    Types.StructType unionPartitionType =
        Partitioning.unionPartitionTypes(current.specsById().values());
    MetricsConfig metricsConfig = MetricsConfig.from(current.properties(), current.schema(), null);
    long snapId = snapshotId();
    boolean hasDvs = bornWithDVByPath != null && !bornWithDVByPath.isEmpty();
    for (DataFile file : files) {
      DeleteFile bornDv = hasDvs ? bornWithDVByPath.get(file.location()) : null;
      TrackedFileAdapters.DataTrackedFile adapter =
          TrackedFileAdapters.forDataFile(
              TableMetadata.MIN_FORMAT_VERSION_ADAPTIVE_MANIFEST_TREE,
              current.schema(),
              metricsConfig,
              unionPartitionType);
      Tracking tracking =
          new TrackingStruct(
              EntryStatus.ADDED,
              snapId,
              explicitDataSequenceNumber,
              null,
              bornDv != null ? snapId : null,
              file.firstRowId(),
              null,
              null);
      TrackedFile row =
          bornDv != null
              ? adapter.wrap(file, tracking, MergingSnapshotProducer.toDeletionVector(bornDv))
              : adapter.wrap(file, tracking);
      addV4AdaptiveNewLiveDataRow(row);
    }
  }

  /**
   * Returns true when the v4+ commit's manifest set is identical to the parent snapshot's manifest
   * set by path. In that case no new manifest content was produced and the parent's root manifest
   * can be reused, avoiding orphan root-manifest files for no-op commits.
   */
  /**
   * Strips the synthetic {@link RootDirectRowsManifestFile} entries from a snapshot's manifest
   * list. Subclasses' {@code apply()} does not include the virtual in its returned manifests, so
   * the parent's list must be normalized the same way before the two are compared.
   */
  private static List<ManifestFile> filterVirtualDirectRows(List<ManifestFile> manifestList) {
    if (manifestList == null || manifestList.isEmpty()) {
      return manifestList;
    }
    List<ManifestFile> filtered = Lists.newArrayListWithCapacity(manifestList.size());
    for (ManifestFile manifest : manifestList) {
      if (!(manifest instanceof RootDirectRowsManifestFile)) {
        filtered.add(manifest);
      }
    }
    return filtered;
  }

  private static boolean isNoOp(List<ManifestFile> manifests, List<ManifestFile> parentManifests) {
    if (manifests.size() != parentManifests.size()) {
      return false;
    }
    Set<String> manifestPaths = Sets.newHashSetWithExpectedSize(manifests.size());
    for (ManifestFile manifest : manifests) {
      manifestPaths.add(manifest.path());
    }
    for (ManifestFile parentManifest : parentManifests) {
      if (!manifestPaths.contains(parentManifest.path())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Computes the number of newly-assigned rows across the given data manifests. On v4+, every data
   * file carries a per-entry {@code first_row_id}, so only newly-added files ({@code
   * addedRowsCount}) consume new row IDs. Existing rows and rewritten (MODIFIED) rows retain their
   * previously-assigned IDs; manifests carried over from ancestor snapshots have their
   * manifest-level {@code first_row_id} already set and contribute nothing further.
   */
  private static long computeAssignedRows(ManifestFile[] manifestFiles) {
    long total = 0L;
    for (ManifestFile manifest : manifestFiles) {
      if (manifest.content() == ManifestContent.DATA
          && manifest.firstRowId() == null
          && manifest.addedRowsCount() != null) {
        total += manifest.addedRowsCount();
      }
    }
    return total;
  }

  private void runValidations(Snapshot parentSnapshot) {
    validate(base, parentSnapshot);

    // Validate snapshot ancestry
    Iterable<Snapshot> snapshotAncestry =
        parentSnapshot != null
            ? SnapshotUtil.ancestorsOf(parentSnapshot.snapshotId(), base::snapshot)
            : List.of();

    boolean valid = snapshotAncestryValidator.validate(snapshotAncestry);
    ValidationException.check(
        valid, "Snapshot ancestry validation failed: %s", snapshotAncestryValidator.errorMessage());
  }

  protected abstract Map<String, String> summary();

  /** Returns the snapshot summary from the implementation and updates totals. */
  private Map<String, String> summary(TableMetadata previous) {
    Map<String, String> summary = summary();

    if (summary == null) {
      return ImmutableMap.of();
    }

    Map<String, String> previousSummary;
    SnapshotRef previousBranchHead = previous.ref(targetBranch);
    if (previousBranchHead != null) {
      if (previous.snapshot(previousBranchHead.snapshotId()).summary() != null) {
        previousSummary = previous.snapshot(previousBranchHead.snapshotId()).summary();
      } else {
        // previous snapshot had no summary, use an empty summary
        previousSummary = ImmutableMap.of();
      }
    } else {
      // if there was no previous snapshot, default the summary to start totals at 0
      ImmutableMap.Builder<String, String> summaryBuilder = ImmutableMap.builder();
      summaryBuilder
          .put(SnapshotSummary.TOTAL_RECORDS_PROP, "0")
          .put(SnapshotSummary.TOTAL_FILE_SIZE_PROP, "0")
          .put(SnapshotSummary.TOTAL_DATA_FILES_PROP, "0")
          .put(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0")
          .put(SnapshotSummary.TOTAL_POS_DELETES_PROP, "0")
          .put(SnapshotSummary.TOTAL_EQ_DELETES_PROP, "0");
      previousSummary = summaryBuilder.build();
    }

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    // copy all summary properties from the implementation
    builder.putAll(summary);

    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_RECORDS_PROP,
        summary,
        SnapshotSummary.ADDED_RECORDS_PROP,
        SnapshotSummary.DELETED_RECORDS_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_FILE_SIZE_PROP,
        summary,
        SnapshotSummary.ADDED_FILE_SIZE_PROP,
        SnapshotSummary.REMOVED_FILE_SIZE_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_DATA_FILES_PROP,
        summary,
        SnapshotSummary.ADDED_FILES_PROP,
        SnapshotSummary.DELETED_FILES_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_DELETE_FILES_PROP,
        summary,
        SnapshotSummary.ADDED_DELETE_FILES_PROP,
        SnapshotSummary.REMOVED_DELETE_FILES_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_POS_DELETES_PROP,
        summary,
        SnapshotSummary.ADDED_POS_DELETES_PROP,
        SnapshotSummary.REMOVED_POS_DELETES_PROP);
    updateTotal(
        builder,
        previousSummary,
        SnapshotSummary.TOTAL_EQ_DELETES_PROP,
        summary,
        SnapshotSummary.ADDED_EQ_DELETES_PROP,
        SnapshotSummary.REMOVED_EQ_DELETES_PROP);

    builder.putAll(EnvironmentContext.get());
    return builder.build();
  }

  protected TableMetadata current() {
    return base;
  }

  protected TableMetadata refresh() {
    this.base = ops.refresh();
    return base;
  }

  @Override
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public void commit() {
    // this is always set to the latest commit attempt's snapshot id.
    AtomicLong newSnapshotId = new AtomicLong(-1L);
    try (Timed ignore = commitMetrics().totalDuration().start()) {
      try {
        Tasks.foreach(ops)
            .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
            .exponentialBackoff(
                base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
                base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
                base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
                2.0 /* exponential */)
            .onlyRetryOn(CommitFailedException.class)
            .countAttempts(commitMetrics().attempts())
            .run(
                taskOps -> {
                  Snapshot newSnapshot = apply();
                  newSnapshotId.set(newSnapshot.snapshotId());
                  TableMetadata.Builder update = TableMetadata.buildFrom(base);
                  if (base.snapshot(newSnapshot.snapshotId()) != null) {
                    // this is a rollback operation
                    update.setBranchSnapshot(newSnapshot.snapshotId(), targetBranch);
                  } else if (stageOnly) {
                    update.addSnapshot(newSnapshot);
                  } else {
                    update.setBranchSnapshot(newSnapshot, targetBranch);
                  }

                  TableMetadata updated = update.build();
                  if (updated.changes().isEmpty()) {
                    // do not commit if the metadata has not changed. for example, this may happen
                    // when setting the current
                    // snapshot to an ID that is already current. note that this check uses
                    // identity.
                    return;
                  }

                  // if the table UUID is missing, add it here. the UUID will be re-created each
                  // time
                  // this operation retries
                  // to ensure that if a concurrent operation assigns the UUID, this operation will
                  // not fail.
                  taskOps.commit(base, updated.withUUID());
                });

      } catch (CommitStateUnknownException commitStateUnknownException) {
        throw commitStateUnknownException;
      } catch (RuntimeException e) {
        if (!strictCleanup || e instanceof CleanableFailure) {
          Exceptions.suppressAndThrow(e, this::cleanAll);
        }

        throw e;
      }

      try {
        LOG.info("Committed snapshot {} ({})", newSnapshotId.get(), getClass().getSimpleName());

        // at this point, the commit must have succeeded. after a refresh, the snapshot is loaded by
        // id in case another commit was added between this commit and the refresh.
        // it might not be known which commit attempt succeeded in some cases, so this only cleans
        // up the one that actually did succeed.
        Snapshot saved = ops.refresh().snapshot(newSnapshotId.get());
        if (saved != null) {
          if (cleanupAfterCommit()) {
            cleanUncommitted(Sets.newHashSet(saved.allManifests(ops.io())));
          }

          // also clean up unused snapshot files (manifest lists for v3, root manifests for v4)
          // created by multiple attempts.
          String committedLocation = saved.snapshotFileLocation();
          for (String snapshotFile : manifestLists) {
            if (!snapshotFile.equals(committedLocation)) {
              deleteFile(snapshotFile);
            }
          }
        } else {
          // saved may not be present if the latest metadata couldn't be loaded due to eventual
          // consistency problems in refresh. in that case, don't clean up.
          LOG.warn("Failed to load committed snapshot, skipping manifest clean-up");
        }
      } catch (Throwable e) {
        LOG.warn(
            "Failed to load committed table metadata or during cleanup, skipping further cleanup",
            e);
      }
    }

    try {
      notifyListeners();
    } catch (Throwable e) {
      LOG.warn("Failed to notify event listeners", e);
    }
  }

  private void notifyListeners() {
    try {
      Object event = updateEvent();
      if (event != null) {
        Listeners.notifyAll(event);

        if (event instanceof CreateSnapshotEvent) {
          CreateSnapshotEvent createSnapshotEvent = (CreateSnapshotEvent) event;

          reporter.report(
              ImmutableCommitReport.builder()
                  .tableName(createSnapshotEvent.tableName())
                  .snapshotId(createSnapshotEvent.snapshotId())
                  .operation(createSnapshotEvent.operation())
                  .sequenceNumber(createSnapshotEvent.sequenceNumber())
                  .metadata(EnvironmentContext.get())
                  .commitMetrics(
                      CommitMetricsResult.from(commitMetrics(), createSnapshotEvent.summary()))
                  .build());
        }
      }
    } catch (RuntimeException e) {
      LOG.warn("Failed to notify listeners", e);
    }
  }

  protected void cleanAll() {
    for (String manifestList : manifestLists) {
      deleteFile(manifestList);
    }
    manifestLists.clear();
    cleanUncommitted(EMPTY_SET);
  }

  protected void deleteFile(String path) {
    deleteFunc.accept(path);
  }

  protected OutputFile manifestListPath() {
    return ops.io()
        .newOutputFile(
            ops.metadataFileLocation(
                FileFormat.AVRO.addExtension(
                    String.format(
                        Locale.ROOT,
                        "snap-%d-%d-%s",
                        snapshotId(),
                        attempt.incrementAndGet(),
                        commitUUID))));
  }

  protected OutputFile rootManifestPath() {
    return ops.io()
        .newOutputFile(
            ops.metadataFileLocation(
                FileFormat.PARQUET.addExtension(
                    String.format(
                        Locale.ROOT,
                        "snap-%d-%d-%s",
                        snapshotId(),
                        attempt.incrementAndGet(),
                        commitUUID))));
  }

  protected EncryptedOutputFile newManifestOutputFile() {
    String manifestFileLocation =
        ops.metadataFileLocation(
            manifestFormat.addExtension(commitUUID + "-m" + manifestCount.getAndIncrement()));
    return EncryptingFileIO.combine(ops.io(), ops.encryption())
        .newEncryptingOutputFile(manifestFileLocation);
  }

  protected ManifestWriter<DataFile> newManifestWriter(PartitionSpec spec) {
    return ManifestFiles.write(
        ops.current().formatVersion(),
        spec,
        newManifestOutputFile(),
        snapshotId(),
        manifestWriterProps);
  }

  protected ManifestWriter<DeleteFile> newDeleteManifestWriter(PartitionSpec spec) {
    return ManifestFiles.writeDeleteManifest(
        ops.current().formatVersion(),
        spec,
        newManifestOutputFile(),
        snapshotId(),
        manifestWriterProps);
  }

  protected RollingManifestWriter<DataFile> newRollingManifestWriter(PartitionSpec spec) {
    return new RollingManifestWriter<>(() -> newManifestWriter(spec), targetManifestSizeBytes);
  }

  protected RollingManifestWriter<DeleteFile> newRollingDeleteManifestWriter(PartitionSpec spec) {
    return new RollingManifestWriter<>(
        () -> newDeleteManifestWriter(spec), targetManifestSizeBytes);
  }

  private static Map<String, String> manifestWriterProperties(TableMetadata metadata) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    String codec =
        metadata.property(
            TableProperties.MANIFEST_COMPRESSION, TableProperties.MANIFEST_COMPRESSION_DEFAULT);
    builder.put(TableProperties.AVRO_COMPRESSION, codec);

    String level =
        metadata.property(
            TableProperties.MANIFEST_COMPRESSION_LEVEL,
            TableProperties.MANIFEST_COMPRESSION_LEVEL_DEFAULT);
    if (level != null) {
      builder.put(TableProperties.AVRO_COMPRESSION_LEVEL, level);
    }

    return builder.build();
  }

  protected ManifestReader<DataFile> newManifestReader(ManifestFile manifest) {
    return ManifestFiles.read(manifest, ops.io(), ops.current().specsById());
  }

  protected ManifestReader<DeleteFile> newDeleteManifestReader(ManifestFile manifest) {
    return ManifestFiles.readDeleteManifest(manifest, ops.io(), ops.current().specsById());
  }

  protected long snapshotId() {
    if (snapshotId == null) {
      synchronized (this) {
        while (snapshotId == null || ops.current().snapshot(snapshotId) != null) {
          this.snapshotId = ops.newSnapshotId();
        }
      }
    }
    return snapshotId;
  }

  protected boolean canInheritSnapshotId() {
    return canInheritSnapshotId;
  }

  protected boolean cleanupAfterCommit() {
    return true;
  }

  /**
   * Builds a snapshot summary with manifest counts.
   *
   * @param manifests the list of manifests in the new snapshot
   * @param replacedManifestsCount the count of manifests that were replaced (rewritten)
   * @return a summary builder with manifest count metrics set
   */
  protected SnapshotSummary.Builder buildManifestCountSummary(
      List<ManifestFile> manifests, int replacedManifestsCount) {
    SnapshotSummary.Builder summaryBuilder = SnapshotSummary.builder();
    int manifestsCreated = 0;
    int manifestsKept = 0;

    for (ManifestFile manifest : manifests) {
      if (snapshotId() == manifest.snapshotId()) {
        manifestsCreated++;
      } else if (null != manifest.snapshotId()) {
        manifestsKept++;
      }
    }

    summaryBuilder.set(SnapshotSummary.CREATED_MANIFESTS_COUNT, String.valueOf(manifestsCreated));
    summaryBuilder.set(SnapshotSummary.KEPT_MANIFESTS_COUNT, String.valueOf(manifestsKept));
    summaryBuilder.set(
        SnapshotSummary.REPLACED_MANIFESTS_COUNT, String.valueOf(replacedManifestsCount));
    return summaryBuilder;
  }

  protected List<ManifestFile> writeDataManifests(Collection<DataFile> files, PartitionSpec spec) {
    return writeDataManifests(files, null /* inherit data seq */, spec);
  }

  protected List<ManifestFile> writeDataManifests(
      Collection<DataFile> files, Long dataSeq, PartitionSpec spec) {
    int groupCount = manifestWriterCount(writePoolParallelism, files.size());
    return ManifestFiles.writeParallel(
        files, groupCount, writePool(), group -> writeDataFileGroup(group, dataSeq, spec));
  }

  // Deletes uncommitted manifests; clears list if clearManifests and any deleted.
  protected void deleteUncommitted(
      Collection<ManifestFile> manifests, Set<ManifestFile> committed, boolean clearManifests) {
    boolean anyDeleted = false;
    for (ManifestFile manifest : manifests) {
      if (!committed.contains(manifest)) {
        deleteFile(manifest.path());
        anyDeleted = true;
      }
    }

    if (clearManifests && anyDeleted) {
      manifests.clear();
    }
  }

  private List<ManifestFile> writeDataFileGroup(
      Collection<DataFile> files, Long dataSeq, PartitionSpec spec) {
    RollingManifestWriter<DataFile> writer = newRollingManifestWriter(spec);

    try (RollingManifestWriter<DataFile> closableWriter = writer) {
      if (dataSeq != null) {
        files.forEach(file -> closableWriter.add(file, dataSeq));
      } else {
        files.forEach(closableWriter::add);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write data manifests");
    }

    return writer.toManifestFiles();
  }

  protected List<ManifestFile> writeDeleteManifests(
      Collection<DeleteFile> files, PartitionSpec spec) {
    int groupCount = manifestWriterCount(writePoolParallelism, files.size());
    return ManifestFiles.writeParallel(
        files, groupCount, writePool(), group -> writeDeleteFileGroup(group, spec));
  }

  private List<ManifestFile> writeDeleteFileGroup(
      Collection<DeleteFile> files, PartitionSpec spec) {
    RollingManifestWriter<DeleteFile> writer = newRollingDeleteManifestWriter(spec);

    try (RollingManifestWriter<DeleteFile> closableWriter = writer) {
      for (DeleteFile file : files) {
        if (file.dataSequenceNumber() != null) {
          closableWriter.add(file, file.dataSequenceNumber());
        } else {
          closableWriter.add(file);
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write delete manifests");
    }

    return writer.toManifestFiles();
  }

  /**
   * Calculates how many manifest writers can be used concurrently to handle the given number of
   * files without creating too small manifests.
   *
   * @param workerPoolSize the size of the available worker pool
   * @param fileCount the total number of files to be processed
   * @return the number of manifest writers that can be used concurrently
   */
  @VisibleForTesting
  static int manifestWriterCount(int workerPoolSize, int fileCount) {
    int limit = IntMath.divide(fileCount, MIN_FILE_GROUP_SIZE, RoundingMode.HALF_UP);
    return Math.max(1, Math.min(workerPoolSize, limit));
  }

  private static ManifestFile addMetadata(TableOperations ops, ManifestFile manifest) {
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, ops.io(), ops.current().specsById())) {
      PartitionSummary stats = new PartitionSummary(ops.current().spec(manifest.partitionSpecId()));
      int addedFiles = 0;
      long addedRows = 0L;
      int existingFiles = 0;
      long existingRows = 0L;
      int deletedFiles = 0;
      long deletedRows = 0L;

      Long snapshotId = null;
      long maxSnapshotId = Long.MIN_VALUE;
      for (ManifestEntry<DataFile> entry : reader.entries()) {
        if (entry.snapshotId() > maxSnapshotId) {
          maxSnapshotId = entry.snapshotId();
        }

        switch (entry.status()) {
          case ADDED:
            addedFiles += 1;
            addedRows += entry.file().recordCount();
            if (snapshotId == null) {
              snapshotId = entry.snapshotId();
            }
            break;
          case EXISTING:
            existingFiles += 1;
            existingRows += entry.file().recordCount();
            break;
          case DELETED:
            deletedFiles += 1;
            deletedRows += entry.file().recordCount();
            if (snapshotId == null) {
              snapshotId = entry.snapshotId();
            }
            break;
        }

        stats.update(entry.file().partition());
      }

      if (snapshotId == null) {
        // if no files were added or deleted, use the largest snapshot ID in the manifest
        snapshotId = maxSnapshotId;
      }

      return new GenericManifestFile(
          manifest.path(),
          manifest.length(),
          manifest.partitionSpecId(),
          ManifestContent.DATA,
          manifest.sequenceNumber(),
          manifest.minSequenceNumber(),
          snapshotId,
          stats.summaries(),
          null,
          addedFiles,
          addedRows,
          existingFiles,
          existingRows,
          deletedFiles,
          deletedRows,
          null);

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read manifest: %s", manifest.path());
    }
  }

  private static void updateTotal(
      ImmutableMap.Builder<String, String> summaryBuilder,
      Map<String, String> previousSummary,
      String totalProperty,
      Map<String, String> currentSummary,
      String addedProperty,
      String deletedProperty) {
    String totalStr = previousSummary.get(totalProperty);
    if (totalStr != null) {
      try {
        long newTotal = Long.parseLong(totalStr);

        String addedStr = currentSummary.get(addedProperty);
        if (newTotal >= 0 && addedStr != null) {
          newTotal += Long.parseLong(addedStr);
        }

        String deletedStr = currentSummary.get(deletedProperty);
        if (newTotal >= 0 && deletedStr != null) {
          newTotal -= Long.parseLong(deletedStr);
        }

        if (newTotal >= 0) {
          summaryBuilder.put(totalProperty, String.valueOf(newTotal));
        }

      } catch (NumberFormatException e) {
        // ignore and do not add total
      }
    }
  }
}
