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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.defaultcolumn.DefaultColumnStatistics;
import org.apache.pinot.segment.local.segment.store.IndexKey;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.MAP_DENSE_INDEX_FILE_EXTENSION;
import static org.apache.pinot.segment.spi.V1Constants.Indexes.MAP_INDEX_FILE_EXTENSION;
import static org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Dense Map Index Creator. This creates <i>Dense</i> indexes for keys. It does this by using existing indexes
 * for any key that is stored in the dense index creator.
 */
public final class MapIndexCreator implements org.apache.pinot.segment.spi.index.creator.MapIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MapIndexCreator.class);

  public static final int VERSION = 1;

  //output file which will hold the range index
  private final String _mapIndexDir;

  private final Map<String, Map<IndexType<?, ?, ?>, IndexCreator>> _creatorsByKeyAndIndex;
  private final TreeMap<String, ColumnIndexCreationInfo> _keyIndexCreationInfoMap = new TreeMap<>();
  private final List<FieldSpec> _denseKeySpecs;
  private final Set<String> _denseKeys;
  private final int _totalDocs;
  private final MapIndexConfig _config;
  private final Map<String, ColumnStatistics> _keyStats = new HashMap<>();
  private final Map<String, ColumnMetadata> _denseKeyMetadata = new HashMap<>();
  private final String _columnName;

  /**
   *
   * @param context The Index Creation Context, used for configuring many of the options for index creation.
   * @param columnName name of the column
   * @throws IOException
   */
  public MapIndexCreator(IndexCreationContext context, String columnName, MapIndexConfig config)
      throws IOException {
    // The Dense map column is composed of other indexes, so we'll store those index in a subdirectory
    // Then when those indexes are created, they are created in this column's subdirectory.
    String indexDir = context.getIndexDir().getPath();
    _config = config;
    _totalDocs = context.getTotalDocs();
    _mapIndexDir = String.format("%s/%s/", indexDir, columnName + MAP_DENSE_INDEX_FILE_EXTENSION);
    _denseKeySpecs = new ArrayList<>(_config.getMaxKeys());
    _denseKeys = new HashSet<>();
    _columnName = columnName;
    for (int i = 0; i < _config.getDenseKeys().size(); i++) {
      FieldSpec keySpec = new DimensionFieldSpec();
      keySpec.setDataType(_config.getDenseKeyTypes().get(i));
      keySpec.setName(_config.getDenseKeys().get(i));
      keySpec.setNullable(true);
      keySpec.setSingleValueField(true);
      keySpec.setDefaultNullValue(null);  // Sets the default default null value
      _denseKeySpecs.add(keySpec);
      _denseKeys.add(keySpec.getName());
    }

    _creatorsByKeyAndIndex = Maps.newHashMapWithExpectedSize(_denseKeySpecs.size());
    buildKeyStats();
    buildIndexCreationInfo();
    createKeyCreators(context);
  }

  private void buildKeyStats() {
    // For each dense key, construct the Column stats for that key
    for (String key : _denseKeys) {
      // Create stats for it
      ColumnStatistics stats = new DefaultColumnStatistics(null, null, null, false, _totalDocs, 0);
      _keyStats.put(key, stats);
    }
  }

  private void createKeyCreators(IndexCreationContext context) {
    final File denseKeyDir = new File(_mapIndexDir);
    try {
      if (!denseKeyDir.mkdirs()) {
        LOGGER.error("Failed to create directory: {}", denseKeyDir);
      }
    } catch (Exception ex) {
      LOGGER.error("Exception while creating temporary directory: '{}'", denseKeyDir, ex);
    }

    for (FieldSpec key : _denseKeySpecs) {
      // Create the context for this dense key
      final String keyName = key.getName();

      boolean dictEnabledColumn =
          false; //createDictionaryForColumn(columnIndexCreationInfo, segmentCreationSpec, fieldSpec);
      ColumnIndexCreationInfo columnIndexCreationInfo = _keyIndexCreationInfoMap.get(key.getName());

      FieldIndexConfigs keyConfig = getKeyIndexConfig(keyName, columnIndexCreationInfo);
      IndexCreationContext.Common keyContext =
          IndexCreationContext.builder().withIndexDir(denseKeyDir).withDictionary(dictEnabledColumn).withFieldSpec(key)
              .withTotalDocs(_totalDocs).withColumnIndexCreationInfo(columnIndexCreationInfo)
              .withLengthOfLongestEntry(10).withOptimizedDictionary(false).onHeap(context.isOnHeap())
              .withForwardIndexDisabled(false).withTextCommitOnClose(true).build();

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
          LOGGER.error("An exception happened while creating IndexCreator for key '{}' for index '{}'", key.getName(),
              index.getId(), e);
        }
      }
      _creatorsByKeyAndIndex.put(key.getName(), creatorsByIndex);
    }
  }

  private FieldIndexConfigs getKeyIndexConfig(String keyName, ColumnIndexCreationInfo columnIndexCreationInfo) {

    FieldIndexConfigs.Builder builder = new FieldIndexConfigs.Builder();
    // Sorted columns treat the 'forwardIndexDisabled' flag as a no-op
    // ForwardIndexConfig fwdConfig = config.getConfig(StandardIndexes.forward());

    ForwardIndexConfig fwdConfig = new ForwardIndexConfig.Builder()
        // TODO (make configurable): .withCompressionCodec(FieldConfig.CompressionCodec.PASS_THROUGH)
        .build();
    // TODO(What's this for?)   if (!fwdConfig.isEnabled() && columnIndexCreationInfo.isSorted()) {
    builder.add(StandardIndexes.forward(), new ForwardIndexConfig.Builder(fwdConfig).build());
    //}
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
    for (FieldSpec key : _denseKeySpecs) {
      String keyName = key.getName();
      DataType storedType = key.getDataType().getStoredType();
      ColumnStatistics columnProfile = null;
      try {
        columnProfile = _keyStats.get(keyName);
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
  public void seal()
      throws IOException {
    for (Map.Entry<String, Map<IndexType<?, ?, ?>, IndexCreator>> keysInMap : _creatorsByKeyAndIndex.entrySet()) {
      for (IndexCreator keyIdxCreator : keysInMap.getValue().values()) {
        keyIdxCreator.seal();
      }
    }

    // Stitch the index files together
    mergeKeyFiles();
  }

  private void mergeKeyFiles() {
    File mergedIndexFile = new File(_mapIndexDir, _columnName + MAP_INDEX_FILE_EXTENSION);

    try {
      long offset = 0;
      final int HEADER_BYTES = 0;
      long totalIndexLength = 0;

      // Compute the total size of the indexes
      List<File> keyFiles = new ArrayList<>(_denseKeys.size());
      for (String key : _denseKeys) {
        File keyFile = getFileFor(key, StandardIndexes.forward());
        totalIndexLength += Files.size(keyFile.toPath());
        keyFiles.add(keyFile);
      }

      // Create an output buffer for writing to (see the V2 to V3 conversion logic for what to do here)
      PinotDataBuffer buffer =
          PinotDataBuffer.mapFile(mergedIndexFile, false, offset, HEADER_BYTES + totalIndexLength, ByteOrder.BIG_ENDIAN,
              null);

      // Iterate over each key and find the index and write the index to a file
      for (File keyFile : keyFiles) {
        try (FileChannel denseKeyFileChannel = new RandomAccessFile(keyFile, "r").getChannel()) {
          long indexSize = denseKeyFileChannel.size();
          try (PinotDataBuffer keyBuffer = PinotDataBuffer.mapFile(keyFile, true, 0, indexSize, ByteOrder.BIG_ENDIAN,
              keyFile.getName())) {
            keyBuffer.copyTo(0, buffer, offset, indexSize);
            offset += indexSize;
          } catch (Exception ex) {
            LOGGER.error("Error opening PinotDataBuffer for '{}'", keyFile, ex);
          }
        } catch (Exception ex) {
          LOGGER.error("Error opening dense key file '{}': ", keyFile, ex);
        }
      }

      // Delete the index files
      buffer.close();

      deleteIntermediateFiles(keyFiles);
    } catch (Exception ex) {
      LOGGER.error("Exception while merging dense key indexes: ", ex);
    }
  }

  private void deleteIntermediateFiles(List<File> files) {
    for (File file : files) {
      try {
        if (!file.delete()) {
          LOGGER.error("Failed to delete file '{}'. Reason is unknown", file);
        }
      } catch (Exception ex) {
        LOGGER.error("Failed to delete intermediate file '{}'", file, ex);
      }
    }
  }

  @Override
  public void add(Map<String, Object> mapValue)
      throws IOException {
    // Iterate over every dense key in this map
    for (FieldSpec denseKey : _denseKeySpecs) {
      String keyName = denseKey.getName();

      // Get the value of the key from the input map and write to each index
      Object value = mapValue.get(keyName);

      // If the value is NULL or the value's type does not match the key's index type then
      // Write the default value to the index
      if (value == null) {
        value = _keyIndexCreationInfoMap.get(keyName).getDefaultNullValue();
      } else {
        DataType valType = convertToDataType(PinotDataType.getSingleValueType(value.getClass()));
        if (!valType.equals(denseKey.getDataType())) {
          LOGGER.warn("Type mismatch, expected '{}' but got '{}'", denseKey.getDataType(), valType);
          value = _keyIndexCreationInfoMap.get(keyName).getDefaultNullValue();
        }
      }

      // Get the type of the value to check that it matches the Dense Key's type
      try {
        // Iterate over each key in the dictionary and if it exists in the record write a value, otherwise write
        // the null value
        for (Map.Entry<IndexType<?, ?, ?>, IndexCreator> indexes : _creatorsByKeyAndIndex.get(keyName).entrySet()) {
          indexes.getValue().add(value, -1); // TODO: Add in dictionary encoding support
        }
      } catch (IOException ioe) {
        LOGGER.error("Error writing to dense key '{}': ", keyName, ioe);
        throw ioe;
      } catch (Exception e) {
        LOGGER.error("Error getting dense key '{}': ", keyName, e);
      }
    }
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

  File getFileFor(String column, IndexType<?, ?, ?> indexType) {
    List<File> candidates = getFilesFor(column, indexType);
    if (candidates.isEmpty()) {
      throw new RuntimeException("No file candidates for index " + indexType + " and column " + column);
    }

    return candidates.stream().filter(File::exists).findAny().orElse(candidates.get(0));
  }

  private List<File> getFilesFor(String key, IndexType<?, ?, ?> indexType) {
    return indexType.getFileExtensions(_denseKeyMetadata.get(key)).stream()
        .map(fileExtension -> new File(_mapIndexDir, key + fileExtension)).collect(Collectors.toList());
  }

  private PinotDataBuffer mapForReads(File file, String context, ReadMode readMode)
      throws IOException {
    Preconditions.checkNotNull(file);
    Preconditions.checkNotNull(context);
    Preconditions.checkArgument(file.exists(), "File: " + file + " must exist");
    Preconditions.checkArgument(file.isFile(), "File: " + file + " must be a regular file");
    String allocationContext = allocationContext(file, context);

    // Backward-compatible: index file is always big-endian
    if (readMode == ReadMode.heap) {
      return PinotDataBuffer.loadFile(file, 0, file.length(), ByteOrder.BIG_ENDIAN, allocationContext);
    } else {
      return PinotDataBuffer.mapFile(file, true, 0, file.length(), ByteOrder.BIG_ENDIAN, allocationContext);
    }
  }

  private String allocationContext(File f, String context) {
    return this.getClass().getSimpleName() + "." + f.toString() + "." + context;
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

  class Header {
    /*
    HEADER
    Version #
    Size of Header
    Number of Dense Keys

    <Dense Key>
    <Data Type>
    <Number of Indexes>
    <Index type> | <Index Offset>
     */

    // Compute the length of the longest key name
    // Compute the size of the header:
    //    size(version) size(header size) size(number keys)
    //    length of longest key * number of keys
    //    1 integer * number of keys  (data type per key)
    //    (1 integer + 1 long) * number of indexes (indexes for each key)

    final int _version = 1;
    int  _headerSize;
    int _numberOfKeys;
    List<KeyEntry> _keys;

    class KeyEntry {
      final String _key;
      final DataType _type;
      final List<IndexType<?,?,?>> _indexes;

      public KeyEntry(String key, DataType type) {
        _key = key;
        _type = type;
        _indexes = new ArrayList<>();
      }

      public void addIndex(IndexType<?,?,?> indexType) {
        _indexes.add(indexType);
      }
    }
  }
}
