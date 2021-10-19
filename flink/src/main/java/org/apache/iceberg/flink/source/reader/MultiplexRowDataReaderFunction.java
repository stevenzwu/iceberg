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
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;

public class MultiplexRowDataReaderFunction extends DataIteratorReaderFunction<TableRecord<RowData>>{
  Map<String, Table> tables;
  Map<String, ScanContext> scanContexts;

  public MultiplexRowDataReaderFunction(
      Configuration config,
      Map<String, Table> tables,
      Map<String, ScanContext> scanContexts) {
    super(new MultiplexArrayPoolDataIteratorBatcher(config, tables));
    this.tables = tables;
    this.scanContexts = scanContexts;
  }

  @Override
  public DataIterator<TableRecord<RowData>> createDataIterator(IcebergSourceSplit split) {
    return new DataIterator<>(
        new MultiplexRowDataFileScanTaskReader(split.fullTableName(),
            new RowDataFileScanTaskReader(
                tables.get(split.fullTableName()).schema(),
                scanContexts.get(split.fullTableName()).project(),
                scanContexts.get(split.fullTableName()).nameMapping(),
                scanContexts.get(split.fullTableName()).caseSensitive())),
        split.task(),
        tables.get(split.fullTableName()).io(),
        tables.get(split.fullTableName()).encryption());
  }
}
