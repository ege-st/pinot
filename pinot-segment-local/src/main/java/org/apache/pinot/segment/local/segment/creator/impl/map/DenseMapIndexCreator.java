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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.jute.Index;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.FieldSpec;
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

  private final HashMap<String, IndexCreator> _denseKeyCreators;

  /**
   *
   * @param indexDir destination of the map index file
   * @param fieldSpec fieldspec of the column to generate the map index
   * @throws IOException
   */
  public DenseMapIndexCreator(String indexDir, FieldSpec fieldSpec, MapIndexConfig config)
      throws IOException {
    Preconditions.checkArgument(fieldSpec.getDataType() == DataType.MAP,
        "Map Index requires the data type to be MAP.");
    Preconditions.checkArgument(fieldSpec.isSingleValueField(),
        "Map Index must be marked as a single value field.");

    String columnName = fieldSpec.getName();

    // The Dense map column is composed of other indexes, so we'll store those index in a subdirectory
    // Then when those indexes are created, they are created in this column's subdirectory.
    _mapIndexDir = String.format("%s/%s/", indexDir, columnName + MAP_DENSE_INDEX_FILE_EXTENSION);
    _denseKeyCreators = new HashMap<>();
  }

  @Override
  public void add(@Nonnull Map<String, Object> mapValue) throws IOException {
    for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
      String entryKey = entry.getKey();
      Object entryVal = entry.getValue();
      DataType valType = convertToDataType(PinotDataType.getSingleValueType(entryVal.getClass()));

      try {
        IndexCreator keyCreator = getKeyCreator(entryKey);
        assert keyCreator != null;
        keyCreator.add(entryVal, -1);
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
    IndexCreator keyCreator = _denseKeyCreators.get(key);
    assert keyCreator != null;
    return keyCreator;
  }

  private IndexCreator createKeyIndex(IndexType type, IndexCreationContext context, IndexConfig config) throws Exception {
    assert type != null;
    assert context != null;
    assert config != null;

    return type.createIndexCreator(context, config);
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

  private static class DenseIndexCreator {
    private final int _offset;
    private final IndexCreator _creator;

    public DenseIndexCreator(IndexCreator creator, int offset) {
      _creator = creator;
      _offset = offset;
    }

    public void add(Object value, int dictId) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
