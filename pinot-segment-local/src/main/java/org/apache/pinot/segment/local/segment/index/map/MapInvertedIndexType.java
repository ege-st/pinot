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

package org.apache.pinot.segment.local.segment.index.map;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.map.MutableMapInvertedIndex;
import org.apache.pinot.segment.local.segment.creator.impl.map.MapIndexCreatorImpl;
import org.apache.pinot.segment.local.segment.creator.impl.map.MapInvertedIndexCreatorImpl;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.JsonIndexHandler;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.MapIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.config.table.MapInvertedIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class MapInvertedIndexType extends AbstractIndexType<MapInvertedIndexConfig, MapIndexReader, MapIndexCreator>
    implements ConfigurableFromIndexLoadingConfig<MapInvertedIndexConfig> {
  public static final String INDEX_DISPLAY_NAME = "map_inverted";
  private static final List<String> EXTENSIONS =
      Collections.singletonList(V1Constants.Indexes.MAP_INVERTED_INDEX_FILE_EXTENSION);

  protected MapInvertedIndexType() {
    super(StandardIndexes.MAP_INVERTED_ID);
  }

  @Override
  public Class<MapInvertedIndexConfig> getIndexConfigClass() {
    return MapInvertedIndexConfig.class;
  }

  @Override
  public Map<String, MapInvertedIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    return indexLoadingConfig.getMapInvertedIndexConfigs();
  }

  @Override
  public MapInvertedIndexConfig getDefaultConfig() {
    return MapInvertedIndexConfig.DISABLED;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<MapInvertedIndexConfig> createDeserializer() {
    /*ColumnConfigDeserializer<IndexConfig> fromInvertedCols = IndexConfigDeserializer.fromCollection(
        tableConfig -> tableConfig.getIndexingConfig().getMapInvertedIndexColumns(),
        (accum, column) -> accum.put(column, IndexConfig.ENABLED));
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass())
        .withExclusiveAlternative(IndexConfigDeserializer.ifIndexingConfig(fromInvertedCols));*/

    //
    ColumnConfigDeserializer<MapInvertedIndexConfig> fromMapInvertedIndexConf =
        IndexConfigDeserializer.fromMap(tableConfig -> tableConfig.getIndexingConfig().getMapInvertedIndexConfigs());

    //
    ColumnConfigDeserializer<MapInvertedIndexConfig> fromMapInvertedIndexCols =
        IndexConfigDeserializer.fromCollection(
            tableConfig -> tableConfig.getIndexingConfig().getMapInvertedIndexColumns(),
            (accum, column) -> accum.put(column, new MapInvertedIndexConfig()));
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass())
        .withExclusiveAlternative(
            IndexConfigDeserializer.ifIndexingConfig(fromMapInvertedIndexCols.withExclusiveAlternative(fromMapInvertedIndexConf)));
  }

  @Override
  public MapIndexCreator createIndexCreator(IndexCreationContext context, MapInvertedIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
        "Map inverted index is currently only supported on single-value columns");
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
        "Map inverted index is currently only supported on STRING columns");
    return new MapInvertedIndexCreatorImpl(
        context.getIndexDir(),
        context.getFieldSpec().getName(),
        context.getTotalDocs(),
        context.getFieldSpec().getDataType()
    );
    /*return context.isOnHeap()
        ? new OnHeapJsonIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(), indexConfig)
        : new OffHeapJsonIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(), indexConfig);
     */
  }

  @Override
  protected IndexReaderFactory<MapIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  public static MapIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IndexReaderConstraintException {
    return ReaderFactory.createIndexReader(dataBuffer, columnMetadata);
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new JsonIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<MapIndexConfig, MapIndexReader> {
    public static final ReaderFactory INSTANCE = new ReaderFactory();

    private ReaderFactory() {
    }

    @Override
    protected IndexType<MapIndexConfig, MapIndexReader, ?> getIndexType() {
      //return StandardIndexes.json();
      throw new UnsupportedOperationException();
    }

    @Override
    protected MapIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        MapIndexConfig indexConfig)
        throws IndexReaderConstraintException {
      return createIndexReader(dataBuffer, metadata);
    }

    public static MapIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IndexReaderConstraintException {
      if (!metadata.getFieldSpec().isSingleValueField()) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.json(),
            "Map index is currently only supported on single-value columns");
      }
      if (metadata.getFieldSpec().getDataType().getStoredType() != FieldSpec.DataType.STRING
        || metadata.getFieldSpec().getDataType().getStoredType() != FieldSpec.DataType.INT) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.json(),
            "Map index is currently only supported on STRING and INT columns");
      }
      //return new ImmutableMapIndexReader(dataBuffer, metadata.getTotalDocs());
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected void handleIndexSpecificCleanup(TableConfig tableConfig) {
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, MapInvertedIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    if (context.getFieldSpec().isSingleValueField() || !context.getFieldSpec().isMapValueField()) {
      return null;
    }
    return new MutableMapInvertedIndex(config);
  }
}
