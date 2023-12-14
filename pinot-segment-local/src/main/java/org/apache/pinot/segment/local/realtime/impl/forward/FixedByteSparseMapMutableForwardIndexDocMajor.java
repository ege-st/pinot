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
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.local.io.reader.impl.FixedByteMapValueMultiColReader;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteMapValueMultiColWriter;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ERICH: This is the PoC class for an index storing the Sparse Map data structure.
 * This PoC is working off a copy of FixedByteSVMutableForwardIndex.
 *
 *
 * This is the doc major data format.  We will build an append only log that stores the
 * DocId, the Key Id, and the Value for the Key
 *
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
public class FixedByteSparseMapMutableForwardIndexDocMajor implements MutableForwardIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSparseMapMutableForwardIndexDocMajor.class);

  // For single writer multiple readers setup, use ArrayList for writer and CopyOnWriteArrayList for reader
  // TODO(ERICH): how does thread-safety work around this? Is there only one thread that can write and many threads
  //   that can read?
  private final FixedByteSVMutableForwardIndex _docIds;
  private final FixedByteSVMutableForwardIndex _keys;
  private final FixedByteSVMutableForwardIndex _values;
  private final HashMap<String, Integer> _keyIds = new HashMap<>();
  private int _nextKeyId = 0;
  private int _nextEntryId = 0;

  private final DataType _storedType;
  private final int _keySizeInBytes = 4;
  private final int _valueSizeInBytes;
  private final int _numRowsPerChunk;
  private final long _chunkSizeInBytes;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;
  private int _capacityInRows = 0;

  /**
   * @param storedType Data type of the values
   * @param fixedLength Fixed length of values if known: only used for BYTES field (HyperLogLog and BigDecimal storage)
   * @param numRowsPerChunk Number of rows to pack in one chunk before a new chunk is created.
   * @param memoryManager Memory manager to be used for allocating memory.
   * @param allocationContext Allocation allocationContext.
   */
  public FixedByteSparseMapMutableForwardIndexDocMajor(DataType storedType, int fixedLength,
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
    _chunkSizeInBytes = numRowsPerChunk * (long)(_valueSizeInBytes + _keySizeInBytes);
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    _keys = new FixedByteSVMutableForwardIndex(false, DataType.INT, DataType.INT.size(),
        numRowsPerChunk, _memoryManager, allocationContext);
    _docIds = new FixedByteSVMutableForwardIndex(false, DataType.INT, DataType.INT.size(),
        numRowsPerChunk, _memoryManager, allocationContext);
    _values = new FixedByteSVMutableForwardIndex(false, storedType, fixedLength,
        numRowsPerChunk, _memoryManager, allocationContext);
  }

  public FixedByteSparseMapMutableForwardIndexDocMajor(DataType valueType, int numRowsPerChunk,
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
   * @param docId - The docId whose map is being updated.
   * @param key - The key to set for the docId in this map.
   * @param value - The value that is associated with the key within this map.
   */
  @Override
  public void setIntMap(int docId, String key, int value) {
    // Get the Key ID
    var keyId = _keyIds.get(key);
    if(keyId == null) {
      keyId = _nextKeyId;
      _keyIds.put(key, _nextKeyId);
      _nextKeyId++;
    }

    _docIds.add(docId, -1, _nextEntryId);
    _keys.add(keyId, -1, _nextEntryId);
    _values.add(value, -1, _nextEntryId);
    _nextEntryId++;
  }

  @Override
  public int getIntMap(int docId, String key) {
    var keyId = _keyIds.get(key);
    if(keyId != null) {
      // Find where docId first occurs in the buffer
      // Check the set of keys for that doc for the given key
      // If found, get the value
      throw new UnsupportedOperationException();
    } else {
      return 0;
    }
  }

  @Override
  public void close()
      throws IOException {
    _keys.close();
    _docIds.close();
    _values.close();
  }

  /**
   * Helper class that encapsulates writer and global startRowId.
   */
  private void addBufferIfNeeded(int row) {
    if (row >= _capacityInRows) {
      // Adding _chunkSizeInBytes in the numerator for rounding up. +1 because rows are 0-based index.
      long buffersNeeded = (row + 1 - _capacityInRows + _numRowsPerChunk) / _numRowsPerChunk;
      for (int i = 0; i < buffersNeeded; i++) {
        // TODO(ERICH): use this so that a key may have multiple buffers as its data set grows.
        //   for now, assuming that a key has as single buffer and if the limit is hit values are just dropped.
        // addBuffer();
      }
    }
  }
}
