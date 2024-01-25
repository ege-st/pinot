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
import java.util.concurrent.ConcurrentMap;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
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
public class FixedByteKeyMajorMapMutableForwardIndex implements MutableForwardIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteKeyMajorMapMutableForwardIndex.class);

  // For single writer multiple readers setup, use ArrayList for writer and CopyOnWriteArrayList for reader
  // TODO(ERICH): how does thread-safety work around this? Is there only one thread that can write and many threads
  //   that can read?
  private final ConcurrentMap<String, FixedByteSVMutableForwardIndex> _keyIndexes = new ConcurrentHashMap<>();  // Change to ConcurrentMap

  // For each key buffer, this records how many rows have been written to that buffer. This is needed because
  // we need to be able to scan a key buffer for a particular doc id.
  private final DataType _storedType;
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
  public FixedByteKeyMajorMapMutableForwardIndex(DataType storedType, int fixedLength,
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

  public FixedByteKeyMajorMapMutableForwardIndex(DataType valueType, int numRowsPerChunk,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    this(valueType, -1, numRowsPerChunk, memoryManager, allocationContext);
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    // TODO(ERICH): This is not used in the ingestion pipeline (that I could tell).  Is it only used in the query engine?
    //   It is used in the segment builder and in the segment loader and the datafetcher.
    return false;
  }

  // TODO(ERICH): this isSingleValue semantics makes dealing with identifying a map value index a challenge b/c
  //   a map value is not a singleValue but we currently use !isSingleValue to identify an array column.  So, I
  //   have to set isSingleValue to false and isMapValue to true and make sure that at every point MV code is _only_
  //   executed when isSingleValue == false and isMapValue == false.
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
    // TODO(ERICH):
    //  - What happens if a key is encountered for the first time after the segment is started, then the
    //    preceding docs (with null values) will be missing?
    //  - What happens when there are docs that have no value for this key? How will their null values get added in?

    // Get the buffer for the given key
    // TODO: get rid of vars :(
    var keyIndex = getOrCreateKeyIndex(key);
    keyIndex.setInt(docId, value);
  }

  private FixedByteSVMutableForwardIndex getOrCreateKeyIndex(String key) {
    return _keyIndexes.computeIfAbsent(
        key, k -> new FixedByteSVMutableForwardIndex(false,
            _storedType, _storedType.size(),
            _numRowsPerChunk, _memoryManager, _allocationContext)
    );
  }

  /**
   *
   *
   * @param docId
   * @param key
   * @return
   */
  @Override
  public int getIntMapKeyValue(int docId, String key) {
    // Replace this with the method that returns a PinotMapArray
    //    The index access/getters and null bit map etc. get methods are on PinotMapArray
    var keyIndex = _keyIndexes.get(key);
    if(keyIndex != null) {
      return keyIndex.getInt(docId);
    } else {
      // TODO(ERICH): if the key does not exist we should return the Null code
      //   Have this handled at the segment builder where the null vector will be kept?
      //   I think that will have to be the case because it will depend on if this a metric map or a dimensional map.
      // TODO(ERICH): comment from Gonzalo.  Usually the null handling. Then we have a null bit map where we can ask
      //   if we have a null or not.  I may not be able to do that trick here.  What we would like to have here is a
      //   method that returns an array of ints.  Have a method that gets the bitmap of nulls for a key.
      //
      // Look for getbitmapnull getnullbitmap or something like that.
      //  Question from Gonzalo: how will we store on disk and how much time will it take to implement.
      //   Suggestion: do not change the fwd index api to get values for keys.  Do this: there is a concept of map and
      //     what we provide from the forward index is just a simple method that:
      //          Map fwd reader is have a method:  getIntAsMap -> Array[PinotMaps] and this Array[PinotMap] is what
      //            we'll use in our operators.  This will be the easiest way to project the values.
      //
      //          PinotMap: getValueOfKey(String) -> [T]
      //
      //          Instead returning [PinotMap]   return PinotMapArray
      //          PinotMapArray: get<T>ValuesOfKey(String) -> [T]
      //                         get<T>NullBitMap(String) -> Bitmap
      //
      //          select col['foo'] ==>
      //        This design should make implementing the fwd index trivial
      //
      //        Add inverted indexes in a similar way to what we have above.  When we instantiate the pinot map array
      //          may have two constructors one for fwd and one that accepts a map of fwd indexes and a map of inverted
      //          indexes
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

  public static class IndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    final ForwardIndexReader<ForwardIndexReaderContext> _fwdIndex;
    final String _key;

    public IndexReader(ForwardIndexReader mapIndex, String key) {
      _fwdIndex = mapIndex;
      _key = key;
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
    public DataType getStoredType() {
      return _fwdIndex.getStoredType();
    }

    @Override
    public int getInt(int docId, ForwardIndexReaderContext context) {
      return _fwdIndex.getIntMapKeyValue(docId, _key);
    }

    @Override
    public int getIntMapKeyValue(int docId, String key) {
      return _fwdIndex.getIntMapKeyValue(docId, key);
    }

    @Override
    public void close()
        throws IOException {

    }
  }
}
