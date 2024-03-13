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
package org.apache.pinot.segment.local.segment.creator.impl.map;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.local.realtime.converter.stats.MutableColumnStatistics;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.MAP_DENSE_INDEX_FILE_EXTENSION;
import static org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Dense Map Index Creator. This creates <i>Dense</i> indexes for keys. It does this by using existing indexes
 * for any key that is stored in the dense index creator.
 */
public final class DenseMapIndexCreator implements org.apache.pinot.segment.spi.index.creator.MapIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DenseMapIndexCreator.class);


  public static final int VERSION = 1;


  //output file which will hold the range index
  private final String _mapIndexDir;

  private int _nextDocId;
  private int _nextValueId;

  private Map<String, Map<IndexType<?, ?, ?>, IndexCreator>> _creatorsByColAndIndex = new HashMap<>();
  private TreeMap<String, ColumnIndexCreationInfo> _keyIndexCreationInfoMap;
  private final List<FieldSpec> _denseKeys;
  //private final List<DataType> _denseKeyTypes;
  private final int _totalDocs;
  private final MapIndexConfig _config;
  private SegmentPreIndexStatsContainer _keyStats;

  /**
   *
   * @param context The Index Creation Context, used for configuring many of the options for index creation.
   * @param columnName name of the column
   * @throws IOException
   */
  public DenseMapIndexCreator(IndexCreationContext context, String columnName, MapIndexConfig config)
      throws IOException {
    // The Dense map column is composed of other indexes, so we'll store those index in a subdirectory
    // Then when those indexes are created, they are created in this column's subdirectory.
    String indexDir = context.getIndexDir().getPath();
    _config = config;
    _totalDocs = context.getTotalDocs();
    _mapIndexDir = String.format("%s/%s/", indexDir, columnName + MAP_DENSE_INDEX_FILE_EXTENSION);
    //_denseKeys = config.getDenseKeys();
    _denseKeys = new ArrayList<>(_config.getMaxKeys());
    for (int i = 0; i < _config.getDenseKeys().size(); i++) {
      FieldSpec keySpec = new DimensionFieldSpec();
      keySpec.setDataType(_config.getDenseKeyTypes().get(i));
      keySpec.setName(_config.getDenseKeys().get(i));
      keySpec.setNullable(false);
      keySpec.setSingleValueField(true);
      keySpec.setDefaultNullValue(null);  // Sets the default default null value
      _denseKeys.add(keySpec);
    }

    //_denseKeyTypes = config.getDenseKeyTypes();
    _creatorsByColAndIndex = Maps.newHashMapWithExpectedSize(_denseKeys.size());
    buildKeyStats();
    buildIndexCreationInfo();
    createKeyCreators(context);
  }

  private void buildKeyStats() {
    // For each dense key, construct the Column stats for that key
    for (FieldSpec key : _denseKeys) {
      String keyName = key.getName();
      // Create stats for it
      // MutableColumnStatistics stats = new MutableColumnStatistics();
    }
  }

  private void createKeyCreators(IndexCreationContext context) {
    for (FieldSpec key : _denseKeys) {
      // Create the context for this dense key
      final String keyName = key.getName();
      File denseKeyDir = new File(String.format("%s/%s", _mapIndexDir, keyName));

      boolean dictEnabledColumn = false; //createDictionaryForColumn(columnIndexCreationInfo, segmentCreationSpec, fieldSpec);
      ColumnIndexCreationInfo columnIndexCreationInfo = _keyIndexCreationInfoMap.get(key.getName());

      FieldIndexConfigs keyConfig = getKeyIndexConfig(keyName, columnIndexCreationInfo);
      IndexCreationContext.Common keyContext = IndexCreationContext.builder()
          .withIndexDir(denseKeyDir)
          .withDictionary(dictEnabledColumn)
          .withFieldSpec(key)
          //.withTotalDocs(segmentIndexCreationInfo.getTotalDocs())
          .withTotalDocs(_totalDocs)
          .withColumnIndexCreationInfo(columnIndexCreationInfo)
          //.withOptimizedDictionary(_config.isOptimizeDictionary()
          //|| _config.isOptimizeDictionaryForMetrics() && fieldSpec.getFieldType() == FieldSpec.FieldType.METRIC)
          .withOptimizedDictionary(false)
          .onHeap(context.isOnHeap())
          //.withForwardIndexDisabled(forwardIndexDisabled)
          .withForwardIndexDisabled(false)
          .withTextCommitOnClose(true)
          .build();

      // Create the forward index creator for this key
      // TODO: Pass index configurations through the MapConfig and then create creators for each index type
      Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex =
          Maps.newHashMapWithExpectedSize(IndexService.getInstance().getAllIndexes().size());
      for (IndexType<?, ?, ?> index : IndexService.getInstance().getAllIndexes()) {
        if (index.getIndexBuildLifecycle() != IndexType.BuildLifecycle.DURING_SEGMENT_CREATION) {
          continue;
        }
        try {
          tryCreateIndexCreator(creatorsByIndex, index, keyContext, keyConfig);
        } catch (Exception e) {
          LOGGER.error("An exception happened while creating IndexCreator for key '{}' for index '{}'", key.getName(), index.getId(), e);
        }
      }
    }
  }

  private FieldIndexConfigs getKeyIndexConfig(String keyName,
        ColumnIndexCreationInfo columnIndexCreationInfo) {

    FieldIndexConfigs.Builder builder = new FieldIndexConfigs.Builder();
    // Sorted columns treat the 'forwardIndexDisabled' flag as a no-op
    // ForwardIndexConfig fwdConfig = config.getConfig(StandardIndexes.forward());

    ForwardIndexConfig fwdConfig = new ForwardIndexConfig.Builder().build();
    if (!fwdConfig.isEnabled() && columnIndexCreationInfo.isSorted()) {
      builder.add(StandardIndexes.forward(),
          new ForwardIndexConfig.Builder(fwdConfig)
              //.withLegacyProperties(segmentCreationSpec.getColumnProperties(), keyName)
              .build());
    }
    // Initialize inverted index creator; skip creating inverted index if sorted
    if (columnIndexCreationInfo.isSorted()) {
      builder.undeclare(StandardIndexes.inverted());
    }
    return builder.build();
  }

  /**
   * Complete the stats gathering process and store the stats information in indexCreationInfoMap.
   */
  void buildIndexCreationInfo()
      throws IOException {
    Set<String> varLengthDictionaryColumns = new HashSet<>(); //new HashSet<>(_config.getVarLengthDictionaryColumns());
    for (FieldSpec key : _denseKeys) {
      String keyName = key.getName();
      DataType storedType = key.getDataType().getStoredType();
      ColumnStatistics columnProfile = null;
      try {
        columnProfile = _keyStats.getColumnProfileFor(keyName);
      } catch (Exception ex) {
        LOGGER.error("Failed to get profile for key: '{}'", keyName, ex);
      }
      boolean useVarLengthDictionary = false;
          //shouldUseVarLengthDictionary(columnName, varLengthDictionaryColumns, storedType, columnProfile);
      Object defaultNullValue = key.getDefaultNullValue();
      if (storedType == DataType.BYTES) {
        defaultNullValue = new ByteArray((byte[]) defaultNullValue);
      }
      boolean createDictionary = false;
          //!rawIndexCreationColumns.contains(keyName) && !rawIndexCompressionTypeKeys.contains(keyName);
      _keyIndexCreationInfoMap.put(keyName,
          new ColumnIndexCreationInfo(columnProfile, createDictionary, useVarLengthDictionary, false/*isAutoGenerated*/,
              defaultNullValue));
    }
  }

  private <C extends IndexConfig> void tryCreateIndexCreator(Map<IndexType<?, ?, ?>, IndexCreator> creatorsByIndex,
      IndexType<C, ?, ?> index, IndexCreationContext.Common context, FieldIndexConfigs fieldIndexConfigs)
      throws Exception {
    C config = fieldIndexConfigs.getConfig(index);
    if (config.isEnabled()) {
      creatorsByIndex.put(index, index.createIndexCreator(context, config));
    }
  }

  @Override
  public void add(@Nonnull Map<String, Object> mapValue) throws IOException {
    for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
      String entryKey = entry.getKey();
      Object entryVal = entry.getValue();
      DataType valType = convertToDataType(PinotDataType.getSingleValueType(entryVal.getClass()));

      try {
        // Iterate over each key in the dictionary and if it exists in the record write a value, otherwise write
        // the null value
        for (Map.Entry<String, Map<IndexType<?,?,?>, IndexCreator>> keysInMap : _creatorsByColAndIndex.entrySet()) {
          String key = keysInMap.getKey();

          for (IndexType<?,?,?> idxType : IndexService.getInstance().getAllIndexes()) {
            IndexCreator keyIdxCreator = keysInMap.getValue().get(idxType);

            Object value = mapValue.get(key);
            if (value != null) {
              keyIdxCreator.add(mapValue.get(key), -1); // TODO: Add in dictionary encoding support
            } else {
              // For dense columns the Mutable Segment should take care of making sure that every entry in the key has a value
              throw new RuntimeException(String.format("Null value found under key: '%s'", key));
            }
          }
        }
      } catch (IOException ioe) {
        LOGGER.error("Error writing to dense key '{}' with type '{}': ", entryKey, valType, ioe);
        throw ioe;
      } catch (Exception e) {
        LOGGER.error("Error getting dense key '{}': ", entryKey);
      }
    }
  }

  private IndexCreator getKeyCreator(String key)
  throws Exception {
    // Check the map for the given key
    // TODO: start with forward index first and then expand to configurable indexes
    IndexCreator keyCreator = _creatorsByColAndIndex.get(key).get(StandardIndexes.forward());
    assert keyCreator != null;
    return keyCreator;
  }

  @Override
  public boolean isDictionaryEncoded(String key) {
    return false;
  }

  @Override
  public DataType getValueType(String key) {
    return null;
  }

  public void close()
      throws IOException {

  }

  static FieldSpec.DataType convertToDataType(PinotDataType ty) {
    // TODO: I've been told that we already have a function to do this, so find that function and replace this
    switch (ty) {
      case BOOLEAN:
        return FieldSpec.DataType.BOOLEAN;
      case SHORT:
      case INTEGER:
        return FieldSpec.DataType.INT;
      case LONG:
        return FieldSpec.DataType.LONG;
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case BIG_DECIMAL:
        return FieldSpec.DataType.BIG_DECIMAL;
      case TIMESTAMP:
        return FieldSpec.DataType.TIMESTAMP;
      case STRING:
        return FieldSpec.DataType.STRING;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private boolean createDictionaryForColumn(ColumnIndexCreationInfo info, SegmentGeneratorConfig config,
      FieldSpec spec) {
    String column = spec.getName();
    boolean createDictionary = false;
    if (config.getRawIndexCreationColumns().contains(column) || config.getRawIndexCompressionType()
        .containsKey(column)) {
      return createDictionary;
    }

    FieldIndexConfigs fieldIndexConfigs = config.getIndexConfigsByColName().get(column);
    if (DictionaryIndexType.ignoreDictionaryOverride(config.isOptimizeDictionary(),
        config.isOptimizeDictionaryForMetrics(), config.getNoDictionarySizeRatioThreshold(), spec, fieldIndexConfigs,
        info.getDistinctValueCount(), info.getTotalNumberOfEntries())) {
      // Ignore overrides and pick from config
      createDictionary = info.isCreateDictionary();
    }
    return createDictionary;
  }
}
