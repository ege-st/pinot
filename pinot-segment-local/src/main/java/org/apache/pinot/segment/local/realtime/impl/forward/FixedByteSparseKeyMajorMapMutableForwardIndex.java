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
package org.apache.pinot.segment.local.realtime.impl.forward;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ERICH: This is the PoC class for an index storing the Sparse Map data structure.
 * This PoC is working off a copy of FixedByteSVMutableForwardIndex.
 *
 *
 * This format is used for dealing with Key major Map (or Dictionary) data storage. Where
 * at the top level a key is mapped to a buffer and in the buffer is the set of Doc Ids that
 * have a value for the Key and what that value is:
 *
 *
 *    | Key |  (Doc Id, Value) |
 *    |-----------------------|
 *    | foo |  (2, 56)        |
 *    |     |  (5, 156)       |
 *    |     |  (15, 336)      |
 *    | bar |  (0, 1)         |
 *    |     |  (4, 10)        |


 * What this will need to handle:
 * 1. Dictionary Encoding for key names and storing that encoding
 * 2. Key major storage of triples: Key[ (DocId, Value)]
 * 3. DocId will need to be found and recorded somehow.
 * 4. DocId will need to be used for look-ups somehow.  We already use DocId for gets so I think I just need to make sure
 * that sets have it.
 * 5. Mapping from Key to the start of its block.  Maybe we use a buffer per key
 * 6. If we use the buffer model then each key will need a list of buffers (following the model in FixedBSVFwdIndex)
 * 7. Add new getter/setter methods ot the MutableForwardIndex interface and implement them here (look at the FixedByteMV...Index)
 * 8. If the MutableFwdIdx interface has anything for aggregateMetrics, then mark it as unsupported in this class (for the PoC)
 */
public class FixedByteSparseKeyMajorMapMutableForwardIndex implements MutableForwardIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSparseKeyMajorMapMutableForwardIndex.class);

  // For single writer multiple readers setup, use ArrayList for writer and CopyOnWriteArrayList for reader
  // TODO(ERICH): how does thread-safety work around this? Is there only one thread that can write and many threads
  //   that can read?
  private final HashMap<String, FixedByteSVMutableForwardIndex> _keyIndexes = new HashMap<>();

  // For each key buffer, this records how many rows have been written to that buffer. This is needed because
  // we need to be able to scan a key buffer for a particular doc id.
  private final ConcurrentHashMap<String, AtomicInteger> _keyBufferSize = new ConcurrentHashMap<>();
  private final DataType _storedType;
  private final int _docIdSizeInBytes = 4;
  private final int _valueSizeInBytes;
  private final int _numRowsPerChunk;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;

  /**
   * @param storedType Data type of the values
   * @param fixedLength Fixed length of values if known: only used for BYTES field (HyperLogLog and BigDecimal storage)
   * @param numRowsPerChunk Number of rows to pack in one chunk before a new chunk is created.
   * @param memoryManager Memory manager to be used for allocating memory.
   * @param allocationContext Allocation allocationContext.
   */
  public FixedByteSparseKeyMajorMapMutableForwardIndex(DataType storedType, int fixedLength,
      int numRowsPerChunk, PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    assert storedType.isFixedWidth();  // TODO(ERICH): see what would trigger this path. For POC should only allow int as the value

    _storedType = storedType;
    if (!storedType.isFixedWidth()) {
      Preconditions.checkState(fixedLength > 0, "Fixed length must be provided for type: %s", storedType);
      _valueSizeInBytes = fixedLength;
    } else {
      _valueSizeInBytes = storedType.size();
    }

    _numRowsPerChunk = numRowsPerChunk;
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
  }

  public FixedByteSparseKeyMajorMapMutableForwardIndex(DataType valueType, int numRowsPerChunk,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    this(valueType, -1, numRowsPerChunk, memoryManager, allocationContext);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public boolean isMapValue() {
    return true;
  }

  @Override
  public DataType getStoredType() {
    return _storedType;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _valueSizeInBytes;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _valueSizeInBytes;
  }

  /**
   * Sets the value of the given key for the given DocId.
   *
   * @param docId - The docId whose map is being updated.
   * @param key - The key to set for the docId in this map.
   * @param value - The value that is associated with the key within this map.
   */
  @Override
  public void setIntMap(int docId, String key, int value) {
    // Get the buffer for the given key
    var keyIndex = getOrCreateKeyIndex(key);
    keyIndex.setInt(docId, value);
  }

  private FixedByteSVMutableForwardIndex getOrCreateKeyIndex(String key) {
    var keyIndex = _keyIndexes.get(key);
    if(keyIndex == null) {
      // If there is no fwd index for the given key then create one
      keyIndex = new FixedByteSVMutableForwardIndex(false, _storedType, _storedType.size(),
          _numRowsPerChunk, _memoryManager, _allocationContext);
      _keyIndexes.put(key, keyIndex);
    }
    return keyIndex;
  }

  /**
   *
   *
   * @param docId
   * @param key
   * @return
   */
  @Override
  public int getIntMapValue(int docId, String key) {
    var keyIndex = _keyIndexes.get(key);
    if(keyIndex != null) {
      return keyIndex.getInt(docId);
    } else {
      // TODO(ERICH): if the key does not exist we should return the Null code
      //   Have this handled at the segment builder where the null vector will be kept?
      //   I think that will have to be the case because it will depend on if this a metric map or a dimensional map.
      return FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_INT;
    }
  }

  @Override
  public Map<String, Integer> getIntMap(int docId) {
    var map = new HashMap<String, Integer>();

    // Iterate through the keys
    for(Map.Entry<String, FixedByteSVMutableForwardIndex> entry : _keyIndexes.entrySet()) {
      // Find each key that has this doc and add to map
      var value = entry.getValue().getInt(docId);
      map.put(entry.getKey(), value);
    }

    return map;
  }

  @Override
  public void close()
      throws IOException {
    for (var keyIndex : _keyIndexes.values()) {
      keyIndex.close();
    }
  }
}
