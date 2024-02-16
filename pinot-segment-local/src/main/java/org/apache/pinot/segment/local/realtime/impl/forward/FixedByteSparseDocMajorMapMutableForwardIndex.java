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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.segment.local.realtime.impl.dictionary.StringOffHeapMutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ERICH: This is the PoC class for an index storing the Sparse Map data structure.
 * This PoC is working off a copy of FixedByteSVMutableForwardIndex.
 *
 * This format stores the values contained in a map space by DocId/KeyId. With the data sorted by DocID then Key
 * ID. To represent this: there are 3 SV indexes: one for DocIDs, One for Key IDs, and one for the Value:
 *
 *   | DocID|  | KeyID |  | Value |
 *   |     0|  |    3  |  |     5 |
 *   |     0|  |    4  |  |    15 |
 *   |     0|  |    9  |  |    -1 |
 *   |     4|  |    3  |  |    12 |
 *   |     5|  |    1  |  |   250 |
 *
 *   Stores the space for a map column where documents 0, 4, and 5 have key/value pairs.  With Doc 0 having {3: 5, 4: 15,
 *   9: -1}, doc 4 having {3: 12}, and doc 5 having {1: 250}.
 *
 *   A separate map is used to store the mapping of Key to Key ID.
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
 *
 */
public class FixedByteSparseDocMajorMapMutableForwardIndex implements MutableForwardIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSparseDocMajorMapMutableForwardIndex.class);

  // For single writer multiple readers setup, use ArrayList for writer and CopyOnWriteArrayList for reader
  // TODO(ERICH): how does thread-safety work around this? Is there only one thread that can write and many threads
  //   that can read?
  private final FixedByteSVMutableForwardIndex _docIds;
  private final FixedByteSVMutableForwardIndex _keys;
  private final FixedByteSVMutableForwardIndex _values;
  private final StringOffHeapMutableDictionary _keysDict;
  private int _nextKeyId = 0;
  private AtomicInteger _nextEntryId = new AtomicInteger(0);

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
  public FixedByteSparseDocMajorMapMutableForwardIndex(DataType storedType, int fixedLength,
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

    // Prepare the key dictionary which provides integer codes for keys.
    _keysDict = new StringOffHeapMutableDictionary(1000, 2000,
        _memoryManager, _allocationContext, 32);
  }

  public FixedByteSparseDocMajorMapMutableForwardIndex(DataType valueType, int numRowsPerChunk,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    this(valueType, -1, numRowsPerChunk, memoryManager, allocationContext);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return false;
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
  public void setIntMapKeyValue(int docId, String key, int value) {
    // Get the Key ID
    var keyId = _keysDict.index(key);

    // The next entry should not be incremented until _after_ the writes are made to avoid dirty reads
    var entryId = _nextEntryId.get();

    _docIds.add(docId, -1, entryId);
    _keys.add(keyId, -1, entryId);
    _values.add(value, -1, entryId);

    // Increment the next entry id counter and assert that it is the successor of the Entry ID that was used for
    // this new entry
    var newNextEntryId = _nextEntryId.incrementAndGet();
    assert newNextEntryId == entryId + 1;
  }

  @Override
  public void setDictIdMapValue(int docId, String[] keys, int[] dictIds) {
    for(int i = 0; i < keys.length; i++ ) {
      setIntMapKeyValue(docId, keys[i], dictIds[i]);
    }
  }

  @Override
  public int getIntMapKeyValue(int docId, String key) {
    var keyId = _keysDict.indexOf(key);

    if(keyId > -1) {
      // TODO(ERICH) Use chunk size to locate which chunk to search within.
      var maxEntryId = _nextEntryId.get();

      // Find where docId first occurs in the buffer
      // Check the set of keys for that doc for the given key
      // If found, get the value
      var lowEntry  = 0;
      var highEntry = maxEntryId;
      while(lowEntry < highEntry) {
        var currentEntry = (highEntry - lowEntry)/2 + lowEntry;
        var currentDocId = _docIds.getInt(currentEntry);
        var currentKeyId = _keys.getInt(currentEntry);
        if(currentDocId == docId) {
          if(currentKeyId == keyId) {
            // Entry is found
            return _values.getInt(currentEntry);
          } else if (currentKeyId < keyId) {
            // We are below the location where the document could be
            lowEntry = currentEntry + 1;
          } else {
            // We are above the location where the document could be
            highEntry = currentEntry;
          }
        } else if(currentDocId < docId) {
          lowEntry = currentEntry + 1;
        } else {
          highEntry = currentEntry;
        }
      }

    }

    // Searched the chunks and did not find the given key
    return 0;
  }

  @Override
  public Map<String, Integer> getIntMap(int docId) {
    // TODO: pass the Out array as aa parameter for this rather than return a Map and return the length of the dictionary
    //   Will need to track teh size of the largest dictionary.  Or use a default max value (if we exceed the map assume\
    //   then drop the keys and log/generate an error)


    // Find the first occurrence of the document id
    // Find where docId first occurs in the buffer
    // Check the set of keys for that doc for the given key
    // If found, get the value
    final var maxEntryId = _nextEntryId.get();
    var lowEntry  = 0;
    var highEntry = maxEntryId;
    var firstOccurrence = -1;
    while(lowEntry < highEntry && firstOccurrence == -1) {
      var currentEntry = (highEntry - lowEntry)/2 + lowEntry;
      var currentDocId = _docIds.getInt(currentEntry);
      if(currentDocId == docId) {
        if(currentDocId == 0) {
          firstOccurrence = currentDocId;
        } else {
          var precedingDocId = _docIds.getInt(currentEntry - 1);
          assert precedingDocId <= currentDocId;
          if(precedingDocId < currentDocId) {
            firstOccurrence = currentDocId;
          } else {
            highEntry = currentEntry;
          }
        }
      } else if(currentDocId < docId) {
        lowEntry = currentEntry + 1;
      } else {
        highEntry = currentEntry;
      }
    }

    // Then scan through all KV pairs in the document
    var map = new HashMap<String, Integer>();
    if(firstOccurrence > -1) {
      var currentEntry = firstOccurrence;
      while(currentEntry < maxEntryId - 1) {
        var keyId = _keys.getInt(currentEntry);
        var key = _keysDict.getStringValue(keyId);
        var value = _values.getInt(currentEntry);
        map.put(key, value);
      }
      return map;
    }

    return map;
  }

  @Override
  public void close()
      throws IOException {
    _keys.close();
    _docIds.close();
    _values.close();
  }
}
