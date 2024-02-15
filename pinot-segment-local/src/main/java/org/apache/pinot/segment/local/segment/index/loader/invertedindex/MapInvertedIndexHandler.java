/**
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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.MapInvertedIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class MapInvertedIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapInvertedIndexHandler.class);

  private final Map<String, MapInvertedIndexConfig> _mapInvertedIndexConfigs;

  public MapInvertedIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      @Nullable TableConfig tableConfig) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig);
    _mapInvertedIndexConfigs = FieldIndexConfigsUtil.enableConfigByColumn(StandardIndexes.map_inverted(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    throw new UnsupportedOperationException();

    /*String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_mapInvertedIndexConfigs.keySet());
    Set<String> existingColumns = segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.map_inverted());
    // Check if any existing index need to be removed.
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Need to remove existing map inverted index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    // Check if any new index need to be added.
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateMapInvertedIndex(columnMetadata)) {
        LOGGER.info("Need to create new json index for segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;*/
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    throw new UnsupportedOperationException();
    // Remove indices not set in table config anymore
    /*String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_mapInvertedIndexConfigs.keySet());
    Set<String> existingColumns = segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.json());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.remove(column)) {
        LOGGER.info("Removing existing json index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.json());
        LOGGER.info("Removed existing json index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddIdx) {
      ColumnMetadata columnMetadata = _segmentDirectory.getSegmentMetadata().getColumnMetadataFor(column);
      if (shouldCreateMapInvertedIndex(columnMetadata)) {
        createMapInvertedIndexForColumn(segmentWriter, columnMetadata);
      }
    }*/
  }

  private boolean shouldCreateMapInvertedIndex(ColumnMetadata columnMetadata) {
    return columnMetadata != null;
  }

  private void createMapInvertedIndexForColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    throw new UnsupportedOperationException();
    /*File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    String columnName = columnMetadata.getColumnName();
    File inProgress = new File(indexDir, columnName + V1Constants.Indexes.MAP_INVERTED_INDEX_FILE_EXTENSION + ".inprogress");
    File mapInvertedIndexFile = new File(indexDir, columnName + V1Constants.Indexes.MAP_INVERTED_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.
      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove inverted map index if exists.
      // For v1 and v2, it's the actual inverted map index. For v3, it's the temporary inverted map index.
      FileUtils.deleteQuietly(mapInvertedIndexFile);
    }

    // Create a temporary forward index if it is disabled and does not exist
    columnMetadata = createForwardIndexIfNeeded(segmentWriter, columnName, true);

    // Create new inverted map index for the column.
    LOGGER.info("Creating new map inverted index for segment: {}, column: {}", segmentName, columnName);
    Preconditions.checkState(columnMetadata.isSingleValue() && (columnMetadata.getDataType() == DataType.STRING
            || columnMetadata.getDataType() == DataType.JSON),
        "Json index can only be applied to single-value STRING or JSON columns");
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(segmentWriter, columnMetadata);
    } else {
      handleNonDictionaryBasedColumn(segmentWriter, columnMetadata);
    }

    // For v3, write the generated json index file into the single file and remove it.
    if (_segmentDirectory.getSegmentMetadata().getVersion() == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(segmentWriter, columnName, mapInvertedIndexFile, StandardIndexes.json());
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created json index for segment: {}, column: {}", segmentName, columnName);*/
  }

  private void handleDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    throw new UnsupportedOperationException();
    /*File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String columnName = columnMetadata.getColumnName();
    IndexCreationContext context = IndexCreationContext.builder()
        .withIndexDir(indexDir)
        .withColumnMetadata(columnMetadata)
        .build();
    JsonIndexConfig config = _mapInvertedIndexConfigs.get(columnName);
    try (ForwardIndexReader forwardIndexReader = ForwardIndexType.read(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        Dictionary dictionary = DictionaryIndexType.read(segmentWriter, columnMetadata);
        JsonIndexCreator jsonIndexCreator = StandardIndexes.json().createIndexCreator(context, config)) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        int dictId = forwardIndexReader.getDictId(i, readerContext);
        jsonIndexCreator.add(dictionary.getStringValue(dictId));
      }
      jsonIndexCreator.seal();
    }*/
  }

  private void handleNonDictionaryBasedColumn(SegmentDirectory.Writer segmentWriter, ColumnMetadata columnMetadata)
      throws Exception {
    throw new UnsupportedOperationException();

    /*File indexDir = _segmentDirectory.getSegmentMetadata().getIndexDir();
    String columnName = columnMetadata.getColumnName();
    IndexCreationContext context = IndexCreationContext.builder()
        .withIndexDir(indexDir)
        .withColumnMetadata(columnMetadata)
        .build();
    MapInvertedIndexConfig config = _mapInvertedIndexConfigs.get(columnName);
    try (ForwardIndexReader forwardIndexReader = ForwardIndexType.read(segmentWriter, columnMetadata);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        MapIndexCreator mapInvertedIndexCreator = StandardIndexes.map_inverted().createIndexCreator(context, config)) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        mapInvertedIndexCreator.add(forwardIndexReader.getString(i, readerContext));
      }
      mapInvertedIndexCreator.seal();
    }*/
  }
}
