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

import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.flink.source.FileScanTaskReader;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.io.CloseableIterator;

public class MultiplexRowDataFileScanTaskReader implements FileScanTaskReader<TableRecord<RowData>> {
  String fullTableName;
  RowDataFileScanTaskReader reader;

  public MultiplexRowDataFileScanTaskReader(String fullTableName, RowDataFileScanTaskReader reader) {
    this.fullTableName = fullTableName;
    this.reader = reader;
  }

  @Override
  public CloseableIterator<TableRecord<RowData>> open(FileScanTask fileScanTask, InputFilesDecryptor inputFilesDecryptor) {
    return null;
  }
}
