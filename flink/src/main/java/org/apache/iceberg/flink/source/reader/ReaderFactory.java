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

package org.apache.iceberg.flink.source.reader;

import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

public interface ReaderFactory<T> extends Serializable {

  /**
   * An iterator over records with their position in the file. The iterator is closeable to
   * support clean resource release and recycling.
   *
   * @param <T> The type of the record.
   */
  interface RecordIterator<T> {

    /**
     * Gets the next record from the file, together with its position.
     *
     * <p>The position information returned with the record point to the record AFTER the
     * returned record, because it defines the point where the reading should resume once the
     * current record is emitted. The position information is put in the source's state when the
     * record is emitted. If a checkpoint is taken directly after the record is emitted, the
     * checkpoint must to describe where to resume the source reading from after that record.
     *
     * <p>Objects returned by this method may be reused by the iterator. By the time that this
     * method is called again, no object returned from the previous call will be referenced any
     * more. That makes it possible to have a single {@link MutableRecordAndPosition} object and
     * return the same instance (with updated record and position) on every call.
     */
    @Nullable
    RecordAndPosition<T> next();

    /**
     * Releases the batch that this iterator iterated over. This is not supposed to close the
     * reader and its resources, but is simply a signal that this iterator is no used any more.
     * This method can be used as a hook to recycle/reuse heavyweight object structures.
     */
    void releaseBatch();
  }

  /**
   * A batch reader for a {@link IcebergSourceSplit}
   *
   * @param <T> output record type
   */
  interface Reader<T> {

    /**
     * Reads one batch. The method should return null when reaching the end of the input. The
     * returned batch will be handed over to the processing threads as one.
     *
     * <p>The returned iterator object and any contained objects may be held onto by the
     * source for some time, so it should not be immediately reused by the reader.
     *
     * <p>To implement reuse and to save object allocation, consider using a {@link
     * org.apache.flink.connector.file.src.util.Pool} and recycle objects into the Pool in the
     * the {@link RecordIterator#releaseBatch()} method.
     */
    @Nullable
    RecordIterator<T> readBatch() throws IOException;

    /**
     * Closes the reader and release all resources
     */
    void close() throws IOException;
  }


  /**
   * Create a batch reader for the input split
   *
   * @param config Flink configuration
   * @param split  Iceberg source split
   * @return a batch reader
   */
  Reader<T> create(Configuration config, IcebergSourceSplit split);

}
