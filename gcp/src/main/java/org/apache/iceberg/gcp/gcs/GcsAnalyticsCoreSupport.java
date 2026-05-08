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
package org.apache.iceberg.gcp.gcs;

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.core.GcsAnalyticsCoreOptions;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.storage.BlobId;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializableSupplier;

/**
 * Holds every reference to {@code com.google.cloud.gcs.analyticscore.*}. This class is only loaded
 * by the JVM when {@code gcs.analytics-core.enabled=true}, so users who do not opt in to the GCS
 * analytics core integration are not required to have {@code gcs-analytics-core} on their runtime
 * classpath.
 */
class GcsAnalyticsCoreSupport implements AutoCloseable {
  private final SerializableSupplier<GcsFileSystem> gcsFileSystemSupplier;
  private transient volatile GcsFileSystem gcsFileSystem;

  GcsAnalyticsCoreSupport(
      Map<String, String> properties, Credentials credentials, String userAgent) {
    ImmutableMap.Builder<String, String> propertiesWithUserAgent =
        new ImmutableMap.Builder<String, String>()
            .putAll(properties)
            .put("gcs.user-agent", userAgent);
    GcsAnalyticsCoreOptions options =
        new GcsAnalyticsCoreOptions("gcs.", propertiesWithUserAgent.build());
    GcsFileSystemOptions fileSystemOptions = options.getGcsFileSystemOptions();
    this.gcsFileSystemSupplier =
        () ->
            credentials == null
                ? new GcsFileSystemImpl(fileSystemOptions)
                : new GcsFileSystemImpl(credentials, fileSystemOptions);
  }

  SeekableInputStream newInputStream(BlobId blobId, Long blobSize, MetricsContext metrics)
      throws IOException {
    GcsFileSystem fs = gcsFileSystem();
    if (null == blobSize) {
      return new GcsInputStreamWrapper(
          GoogleCloudStorageInputStream.create(fs, gcsItemId(blobId)), blobId, metrics);
    }

    return new GcsInputStreamWrapper(
        GoogleCloudStorageInputStream.create(fs, gcsFileInfo(blobId, blobSize)), blobId, metrics);
  }

  GcsFileSystem gcsFileSystem() {
    if (gcsFileSystem == null) {
      synchronized (this) {
        if (gcsFileSystem == null) {
          this.gcsFileSystem = gcsFileSystemSupplier.get();
        }
      }
    }

    return gcsFileSystem;
  }

  private static GcsItemId gcsItemId(BlobId blobId) {
    GcsItemId.Builder builder =
        GcsItemId.builder().setBucketName(blobId.getBucket()).setObjectName(blobId.getName());
    if (blobId.getGeneration() != null) {
      builder.setContentGeneration(blobId.getGeneration());
    }

    return builder.build();
  }

  private static GcsFileInfo gcsFileInfo(BlobId blobId, long size) {
    GcsItemId itemId = gcsItemId(blobId);
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(size).build();
    return GcsFileInfo.builder()
        .setItemInfo(itemInfo)
        .setUri(URI.create(blobId.toGsUtilUri()))
        .setAttributes(ImmutableMap.of())
        .build();
  }

  @Override
  public void close() throws IOException {
    GcsFileSystem fs = gcsFileSystem;
    if (fs != null) {
      fs.close();
    }
  }
}
