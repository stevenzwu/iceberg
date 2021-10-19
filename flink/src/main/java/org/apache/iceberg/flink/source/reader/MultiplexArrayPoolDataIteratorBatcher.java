/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.source.reader;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.io.CloseableIterator;

public class MultiplexArrayPoolDataIteratorBatcher<T> implements DataIteratorBatcher<T> {

  MultiplexArrayPoolDataIteratorBatcher(Configuration config,
                                        Map<String, Table> tables) {

  }

  @Override
  public CloseableIterator<RecordsWithSplitIds<RecordAndPosition<T>>> apply(String splitId, DataIterator<T> inputIterator) {
    return null;
  }
}
