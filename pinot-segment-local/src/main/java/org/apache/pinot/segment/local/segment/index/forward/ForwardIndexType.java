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

package org.apache.pinot.segment.local.segment.index.forward;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteMVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSparseMapMutableForwardIndexKeyMajor;
import org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.ForwardIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class ForwardIndexType extends AbstractIndexType<ForwardIndexConfig, ForwardIndexReader, ForwardIndexCreator>
    implements ConfigurableFromIndexLoadingConfig<ForwardIndexConfig> {
  public static final String INDEX_DISPLAY_NAME = "forward";
  // For multi-valued column, forward-index.
  // Maximum number of multi-values per row. We assert on this.
  private static final int MAX_MULTI_VALUES_PER_ROW = 1000;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT = 100;
  private static final int NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT = 100_000;
  private static final List<String> EXTENSIONS = Lists.newArrayList(
      V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION,
      V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION
  );

  protected ForwardIndexType() {
    super(StandardIndexes.FORWARD_ID);
  }

  @Override
  public Class<ForwardIndexConfig> getIndexConfigClass() {
    return ForwardIndexConfig.class;
  }

  @Override
  public Map<String, ForwardIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    Set<String> disabledColumns = indexLoadingConfig.getForwardIndexDisabledColumns();
    Map<String, CompressionCodec> compressionCodecMap = indexLoadingConfig.getCompressionConfigs();
    Map<String, ForwardIndexConfig> result = new HashMap<>();
    for (String column : indexLoadingConfig.getAllKnownColumns()) {
      ForwardIndexConfig forwardIndexConfig;
      if (!disabledColumns.contains(column)) {
        CompressionCodec compressionCodec = compressionCodecMap.get(column);
        if (compressionCodec == null) {
          TableConfig tableConfig = indexLoadingConfig.getTableConfig();
          if (tableConfig != null && tableConfig.getFieldConfigList() != null) {
            FieldConfig fieldConfig =
                tableConfig.getFieldConfigList().stream().filter(fc -> fc.getName().equals(column)).findAny()
                    .orElse(null);
            if (fieldConfig != null) {
              compressionCodec = fieldConfig.getCompressionCodec();
            }
          }
        }
        forwardIndexConfig = new ForwardIndexConfig.Builder().withCompressionCodec(compressionCodec).build();
      } else {
        forwardIndexConfig = ForwardIndexConfig.DISABLED;
      }
      result.put(column, forwardIndexConfig);
    }
    return result;
  }

  @Override
  public ForwardIndexConfig getDefaultConfig() {
    return ForwardIndexConfig.DEFAULT;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<ForwardIndexConfig> createDeserializer() {
    // reads tableConfig.fieldConfigList and decides what to create using the FieldConfig properties and encoding
    ColumnConfigDeserializer<ForwardIndexConfig> fromOld = (tableConfig, schema) -> {
      Map<String, DictionaryIndexConfig> dictConfigs = StandardIndexes.dictionary().getConfig(tableConfig, schema);

      Map<String, ForwardIndexConfig> fwdConfig = Maps.newHashMapWithExpectedSize(
          Math.max(dictConfigs.size(), schema.size()));

      Collection<FieldConfig> fieldConfigs = tableConfig.getFieldConfigList();
      if (fieldConfigs != null) {
        for (FieldConfig fieldConfig : fieldConfigs) {
          Map<String, String> properties = fieldConfig.getProperties();
          if (properties != null && isDisabled(properties)) {
            fwdConfig.put(fieldConfig.getName(), ForwardIndexConfig.DISABLED);
          } else {
            ForwardIndexConfig config = createConfigFromFieldConfig(fieldConfig);
            if (!config.equals(ForwardIndexConfig.DEFAULT)) {
              fwdConfig.put(fieldConfig.getName(), config);
            }
            // It is important to do not explicitly add the default value here in order to avoid exclusive problems with
            // the default `fromIndexes` deserializer.
          }
        }
      }
      return fwdConfig;
    };
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass())
        .withExclusiveAlternative(fromOld);
  }

  private boolean isDisabled(Map<String, String> props) {
    return Boolean.parseBoolean(
        props.getOrDefault(FieldConfig.FORWARD_INDEX_DISABLED, FieldConfig.DEFAULT_FORWARD_INDEX_DISABLED));
  }

  private ForwardIndexConfig createConfigFromFieldConfig(FieldConfig fieldConfig) {
    ForwardIndexConfig.Builder builder = new ForwardIndexConfig.Builder();
    builder.withCompressionCodec(fieldConfig.getCompressionCodec());
    Map<String, String> properties = fieldConfig.getProperties();
    if (properties != null) {
      builder.withLegacyProperties(properties);
    }
    return builder.build();
  }

  public static ChunkCompressionType getDefaultCompressionType(FieldSpec.FieldType fieldType) {
    if (fieldType == FieldSpec.FieldType.METRIC) {
      return ChunkCompressionType.PASS_THROUGH;
    } else {
      return ChunkCompressionType.LZ4;
    }
  }

  @Override
  public ForwardIndexCreator createIndexCreator(IndexCreationContext context, ForwardIndexConfig indexConfig)
      throws Exception {
    return ForwardIndexCreatorFactory.createIndexCreator(context, indexConfig);
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new ForwardIndexHandler(segmentDirectory, configsByCol, schema, tableConfig);
  }

  @Override
  protected IndexReaderFactory<ForwardIndexReader> createReaderFactory() {
    return ForwardIndexReaderFactory.getInstance();
  }

  public String getFileExtension(ColumnMetadata columnMetadata) {
    if (columnMetadata.isSingleValue()) {
      if (!columnMetadata.hasDictionary()) {
        return V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else if (columnMetadata.isSorted()) {
        return V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else {
        return V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      }
    } else if (!columnMetadata.hasDictionary()) {
      return V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    } else {
      return V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION;
    }
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    if (columnMetadata == null) {
      return EXTENSIONS;
    }
    return Collections.singletonList(getFileExtension(columnMetadata));
  }

  /**
   * Returns the forward index reader for the given column.
   *
   * This method will return the default reader, skipping any index overload.
   */
  public static ForwardIndexReader<?> read(SegmentDirectory.Reader segmentReader,
      ColumnMetadata columnMetadata)
      throws IOException {
    PinotDataBuffer dataBuffer = segmentReader.getIndexFor(columnMetadata.getColumnName(), StandardIndexes.forward());
    return read(dataBuffer, columnMetadata);
  }

  /**
   * Returns the forward index reader for the given column.
   *
   * This method will return the default reader, skipping any index overload.
   */
  public static ForwardIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata) {
    return ForwardIndexReaderFactory.createIndexReader(dataBuffer, metadata);
  }

  /**
   * Returns the forward index reader for the given column.
   *
   * This method will delegate on {@link StandardIndexes}, so the correct reader will be returned even when using
   * index overload.
   */
  public static ForwardIndexReader<?> read(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
      ColumnMetadata metadata)
      throws IndexReaderConstraintException, IOException {
    return StandardIndexes.forward().getReaderFactory().createIndexReader(segmentReader, fieldIndexConfigs, metadata);
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, ForwardIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    String column = context.getFieldSpec().getName();
    String segmentName = context.getSegmentName();
    FieldSpec.DataType storedType = context.getFieldSpec().getDataType().getStoredType();
    int fixedLengthBytes = context.getFixedLengthBytes();
    boolean isSingleValue = context.getFieldSpec().isSingleValueField();
    var isMapValue = context.getFieldSpec().isMapValueField();
    if (!context.hasDictionary()) {
      // TODO(ERICH): Expand in here or in the "HAS DICTIONARY" branch?
      // TODO(ERICH): Add branch here for isMapValue
      if (isSingleValue) {
        String allocationContext =
            IndexUtil.buildAllocationContext(context.getSegmentName(), context.getFieldSpec().getName(),
                V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
        if (storedType.isFixedWidth() || fixedLengthBytes > 0) {
          return new FixedByteSVMutableForwardIndex(false, storedType, fixedLengthBytes, context.getCapacity(),
              context.getMemoryManager(), allocationContext);
        } else {
          // RealtimeSegmentStatsHistory does not have the stats for no-dictionary columns from previous consuming
          // segments
          // TODO: Add support for updating RealtimeSegmentStatsHistory with average column value size for no dictionary
          //       columns as well
          // TODO: Use the stats to get estimated average length
          // Use a smaller capacity as opposed to segment flush size
          int initialCapacity = Math.min(context.getCapacity(),
              NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT);
          return new VarByteSVMutableForwardIndex(storedType, context.getMemoryManager(), allocationContext,
              initialCapacity, NODICT_VARIABLE_WIDTH_ESTIMATED_AVERAGE_VALUE_LENGTH_DEFAULT);
        }
      } else if (isMapValue) {
        assert storedType.isFixedWidth();

        String allocationContext =
            IndexUtil.buildAllocationContext(context.getSegmentName(), context.getFieldSpec().getName(),
                V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
        int initialCapacity = Math.min(context.getCapacity(),
            NODICT_VARIABLE_WIDTH_ESTIMATED_NUMBER_OF_VALUES_DEFAULT);
        return new FixedByteSparseMapMutableForwardIndexKeyMajor(
            storedType, storedType.size(), initialCapacity, context.getMemoryManager(), allocationContext);
      } else {
        // TODO: Add support for variable width (bytes, string, big decimal) MV RAW column types
        assert storedType.isFixedWidth();
        String allocationContext =
            IndexUtil.buildAllocationContext(context.getSegmentName(), context.getFieldSpec().getName(),
                V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);
        // TODO: Start with a smaller capacity on FixedByteMVForwardIndexReaderWriter and let it expand
        return new FixedByteMVMutableForwardIndex(MAX_MULTI_VALUES_PER_ROW, context.getAvgNumMultiValues(),
            context.getCapacity(), storedType.size(), context.getMemoryManager(), allocationContext, false,
            storedType);
      }
    } else {
      // TODO(ERICH): What is dictionary used for?  Could it be off-heap related?
      if (isSingleValue) {
        String allocationContext = IndexUtil.buildAllocationContext(segmentName, column,
            V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
        return new FixedByteSVMutableForwardIndex(true, FieldSpec.DataType.INT, context.getCapacity(),
            context.getMemoryManager(), allocationContext);
      } else {
        String allocationContext = IndexUtil.buildAllocationContext(segmentName, column,
            V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION);
        // TODO: Start with a smaller capacity on FixedByteMVForwardIndexReaderWriter and let it expand
        return new FixedByteMVMutableForwardIndex(MAX_MULTI_VALUES_PER_ROW, context.getAvgNumMultiValues(),
            context.getCapacity(), Integer.BYTES, context.getMemoryManager(), allocationContext, true,
            FieldSpec.DataType.INT);
      }
    }
  }
}
