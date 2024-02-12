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
import org.apache.pinot.segment.local.segment.creator.impl.map.MapIndexCreatorImpl;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.JsonIndexHandler;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
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
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;

public class MapIndexType extends AbstractIndexType<MapIndexConfig, MapIndexReader, MapIndexCreator>
    implements ConfigurableFromIndexLoadingConfig<MapIndexConfig> {
  public static final String INDEX_DISPLAY_NAME = "map";
  private static final List<String> EXTENSIONS =
      Collections.singletonList(V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION);

  protected MapIndexType() {
    super(StandardIndexes.MAP_ID);
  }

  @Override
  public Class<MapIndexConfig> getIndexConfigClass() {
    return MapIndexConfig.class;
  }

  @Override
  public Map<String, MapIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    return indexLoadingConfig.getMapIndexConfigs();
  }

  @Override
  public MapIndexConfig getDefaultConfig() {
    return MapIndexConfig.DISABLED;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<MapIndexConfig> createDeserializer() {
    // reads tableConfig.indexingConfig.jsonIndexConfigs
    throw new UnsupportedOperationException();
  }

  @Override
  public MapIndexCreator createIndexCreator(IndexCreationContext context, MapIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
        "Json index is currently only supported on single-value columns");
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
        "Json index is currently only supported on STRING columns");
    return new MapIndexCreatorImpl(
        context.getIndexDir(),
        context.getFieldSpec().getName(),
        context.getTotalDocs(),
        context.getFieldSpec().getDataType(),
        indexConfig.getMaxKeysPerDoc()
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
    tableConfig.getIndexingConfig().setJsonIndexColumns(null);
    tableConfig.getIndexingConfig().setJsonIndexConfigs(null);
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, MapIndexConfig config) {
    if (config.isDisabled()) {
      return null;
    }
    if (!context.getFieldSpec().isSingleValueField()) {
      return null;
    }
    //return new MutableJsonIndexImpl(config);
    throw new UnsupportedOperationException();
  }
}
