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
package org.apache.iceberg.flink;

import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Process-wide registry that tracks live {@link FlinkCatalog} instances by catalog name.
 *
 * <p>The {@link FlinkDynamicTableFactory} uses this registry to resolve a source Iceberg catalog by
 * name when it needs to load an Iceberg table whose Flink-side {@link
 * org.apache.flink.table.catalog.CatalogTable} carries only a catalog name reference (rather than
 * an embedded copy of the source catalog's connection properties). The registry is populated when a
 * {@link FlinkCatalog} is constructed and cleared on {@link FlinkCatalog#close()}.
 *
 * <p>This is in-process state. A {@link FlinkCatalog} must be registered in the current Flink
 * session for by-name resolution to work. Sessions that load a table whose source catalog is not
 * registered fall back to legacy behavior or fail with an actionable error.
 */
class FlinkCatalogRegistry {

  private static final ConcurrentMap<String, FlinkCatalog> CATALOGS = Maps.newConcurrentMap();

  private FlinkCatalogRegistry() {}

  static void register(String name, FlinkCatalog catalog) {
    CATALOGS.put(name, catalog);
  }

  static void unregister(String name, FlinkCatalog catalog) {
    // Only remove the entry if it still points at the given instance, so a re-registered catalog
    // with the same name is not accidentally unregistered when an older instance is closed.
    CATALOGS.remove(name, catalog);
  }

  static FlinkCatalog get(String name) {
    return CATALOGS.get(name);
  }
}
