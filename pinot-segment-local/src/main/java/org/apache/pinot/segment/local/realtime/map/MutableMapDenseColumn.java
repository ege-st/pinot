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

package org.apache.pinot.segment.local.realtime.map;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableMapIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dynamically typed Dense map column. This structure will allow for a "partially" dynamically typed map value
 * to be created where different keys may have different types.  The type of the key is determined when the
 * key is first added to the index.
 *
 * Note, that this means that the type of a key can change across segments.
 */
public class MutableMapDenseColumn implements MutableMapIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableMapDenseColumn.class);
  private final ConcurrentHashMap<String, MutableForwardIndex> _keyIndexes;
  private final int _maxKeys;
  private final boolean _offHeap;
  private final int _capacity;
  private final boolean _isDictionary;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final File _consumerDir;
  private final String _segmentName;

  public MutableMapDenseColumn(int maxKeys, PinotDataBufferMemoryManager memoryManager, int capacity, boolean offHeap,
      boolean isDictionary, String consumerDir, String segmentName) {
    LOGGER.info("Creating Mutable Map Dense Column. Max Keys: {}, Capacity: {}, offHeap: {}, isDictionary: {}, consumerDir: {}, Segment name: {}",
        maxKeys, capacity, offHeap, isDictionary, consumerDir, segmentName);
    _maxKeys = maxKeys;
    _keyIndexes = new ConcurrentHashMap<>();
    _memoryManager = memoryManager;
    _capacity = capacity;
    _offHeap = offHeap;
    _isDictionary = isDictionary;
    _consumerDir = consumerDir != null ? new File(consumerDir) : null;
    _segmentName = segmentName;
  }

  /**
   * Adds a single map value to the Index.
   *
   * This will iterate over each Key-Value pair in <i>value</i> and will add the Key and Value to the index for
   * <i>docIde</i>. When adding a Key-Value pair (<i>K</i> and <i>V</i>), this will check to see if this is the first
   * time <i>K</i> has appeared in the index: if it is, then the type of <i>V</i> will be used to dynamically determine the type of
   * the key. If the key is already in the index, then this will check that the type of <i>V</i> matches the already
   * determined type for <i>K</i>.
   *
   * @param value A nonnull map value to be added to the index.
   * @param docId The document id of the given row. A non-negative value.
   */
  @Override
  public void add(Map<String, Object> value, int docId) {
    assert value != null;
    // Iterate over the KV pairs in the document
    for(Map.Entry<String, Object> entry : value.entrySet()) {
      String key = entry.getKey();
      Object val = entry.getValue();
      FieldSpec.DataType valType = convertToDataType(PinotDataType.getSingleValueType(val.getClass()));

      // Get the index for the key
      MutableForwardIndex keyIndex = getKeyIndex(key, valType, docId);

      // Add the value to the index
      keyIndex.add(val, -1, docId);
    }
  }

  @Override
  public void add(Map<String, Object>[] values, int[] docIds) {
    assert values.length == docIds.length;

    for(int i = 0; i < values.length; i++) {
      add(values[i], docIds[i]);
    }
  }

  private MutableForwardIndex getKeyIndex(String key, FieldSpec.DataType type, int docIdOffset) {
    // Check to see if the index exists
    MutableForwardIndex keyIndex = _keyIndexes.get(key);
    if (keyIndex != null) {
      // If it does, then check to see if the type of the index matches the type of the value that is being added
      if (keyIndex.getStoredType().equals(type)) {
        return keyIndex;
      } else {
        // If the types do not match, throw an exception, if the types do match then return the index
        throw new RuntimeException(
            String.format("Attempting to write a value of type %s to a key of type %s",
                type.toString(),
                keyIndex.getStoredType().toString()));
      }
    } else {
      // If the key does not have an index, then create an index for the given value
      MutableForwardIndex idx = createKeyIndex(key, type, docIdOffset);
      _keyIndexes.put(key, idx);
      return idx;
    }
  }

  MutableForwardIndex createKeyIndex(String key, FieldSpec.DataType type, int docIdOffset) {
    if (_keyIndexes.size() >= _maxKeys) {
      throw new RuntimeException(String.format("Maximum number of keys exceed: %d", _maxKeys));
    }

    LOGGER.info("Creating new Dense Column for key {} with type {}", key, type);

    FieldSpec fieldSpec= new DimensionFieldSpec(key, type, true);
    MutableIndexContext context =
        MutableIndexContext.builder().withFieldSpec(fieldSpec).withMemoryManager(_memoryManager)
            .withDictionary(_isDictionary).withCapacity(_capacity).offHeap(_offHeap).withSegmentName(_segmentName)
            .withConsumerDir(_consumerDir)
            .withFixedLengthBytes(-1).build();  // TODO: judging by the MutableSegmentImpl this would be -1 but should double check
    FieldIndexConfigs indexConfig = FieldIndexConfigs.EMPTY;
    MutableForwardIndex idx =  createMutableForwardIndex(StandardIndexes.forward(), context, indexConfig);
    return new DenseColumn(idx, docIdOffset);
  }

  private MutableForwardIndex createMutableForwardIndex(IndexType<ForwardIndexConfig, ?, ?> indexType, MutableIndexContext context, FieldIndexConfigs indexConfigs) {
    return (MutableForwardIndex) indexType.createMutableIndex(context, indexConfigs.getConfig(StandardIndexes.forward()));
  }

  FieldSpec.DataType convertToDataType(PinotDataType ty) {
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

  @Override
  public ForwardIndexReader<ForwardIndexReaderContext> getKeyReader(String key) {
      return _keyIndexes.get(key);
  }

  @Override
  public void close()
      throws IOException {
    // Iterate over each index and close them
    for(MutableForwardIndex idx : _keyIndexes.values()) {
      idx.close();
    }
  }

  /**
   * A wrapper class around a Dense Mutable Forward index. This is necessary because a dense forward index may have
   * a Doc ID Offset.
   */
  private static class DenseColumn implements MutableForwardIndex {
    // A key may be added to the index after the first document. In which case, when the Forward index for that key
    // is created, the docIds for this index will not begin with 0, but they will be stored in the index with docId
    // 0.  This value will track the offset that will be used to account for this.
    private final int _firstDocId;
    private final MutableForwardIndex _idx;

    public DenseColumn(MutableForwardIndex idx, int firstDocId) {
      _idx = idx;
      _firstDocId = firstDocId;
    }

    /**
     * Adjusts the Requested Document ID by the ID Offset of this column so that it indexes into the internal
     * column correctly.
     *
     * @param docId
     * @return
     */
    private int getInternalDocId(int docId) {
      return docId - _firstDocId;
    }

    @Override
    public int getLengthOfShortestElement() {
      return _idx.getLengthOfShortestElement();
    }

    @Override
    public int getLengthOfLongestElement() {
      return _idx.getLengthOfLongestElement();
    }

    @Override
    public boolean isDictionaryEncoded() {
      return _idx.isDictionaryEncoded();
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public FieldSpec.DataType getStoredType() {
      return _idx.getStoredType();
    }

    @Override
    public void add(@Nonnull Object value, int dictId, int docId) {
      // Account for the docId offset that will happen when new columns are added after the segment has started
      int adjustedDocId = getInternalDocId(docId);
      _idx.add(value, dictId, adjustedDocId);
    }

    @Override
    public void add(@Nonnull Object[] value, @Nullable int[] dictIds, int docId) {
      throw new UnsupportedOperationException("Multivalues are not yet supported in Maps");
    }

    @Override
    public double getDouble(int docId) {
      int adjustedDocId = getInternalDocId(docId);
      return _idx.getDouble(adjustedDocId);
    }

    @Override
    public double getDouble(int docId, ForwardIndexReaderContext context) {
      int adjustedDocId = getInternalDocId(docId);
      return _idx.getDouble(adjustedDocId, context);
    }

    @Override
    public int getInt(int docId) {
      int adjustedDocId = getInternalDocId(docId);
      return _idx.getInt(adjustedDocId);
    }

    @Override
    public int getInt(int docId, ForwardIndexReaderContext context) {
      int adjustedDocId = getInternalDocId(docId);
      return _idx.getInt(adjustedDocId, context);
    }

    @Override
    public String getString(int docId) {
      int adjustedDocId = getInternalDocId(docId);
      return _idx.getString(adjustedDocId);
    }

    @Override
    public String getString(int docId, ForwardIndexReaderContext context) {
      int adjustedDocId = getInternalDocId(docId);
      return _idx.getString(adjustedDocId, context);
    }

    @Override
    public void close()
        throws IOException {
      _idx.close();
    }
  }
}
