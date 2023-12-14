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
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
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
public class FixedByteSparseMapMutableForwardIndex implements MutableForwardIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSparseMapMutableForwardIndex.class);

  // For single writer multiple readers setup, use ArrayList for writer and CopyOnWriteArrayList for reader
  // TODO(ERICH): how does thread-safety work around this? Is there only one thread that can write and many threads
  //   that can read?
  private final HashMap<String, KeyValueWriterWithOffset> _keyWriters = new HashMap<>();
  // TODO(ERICH): This needs to be thread safe (I'm assuming from the CoW), what to change to for the hashmap?
  //    Use a ConcurrentHashMap?
  //    eg: private final ConcurrentHashMap<Integer, KeyValueReaderWithOffset> _readers = new ConcurrentHashMap<>();
  //    ** Note to self: I think we need to be careful: if a Query thread is reading this Map column it might happen
  //        that a Doc adds a key while the read is happening and there is "read skew" (right term?) where at one point
  //        the query reads for the key for a doc and gets None and then later gets a value for that key.
  //        Actually: I think worrying about this is pointless because at the BUFFER level we have no such protection
  //          so if a new (DocId,Value) is added to a Key's buffer it will show up in the middle of a query execution
  //          and create skew.
  //        Will this matter for anything other than joins?
  private final ConcurrentHashMap<String, KeyValueReaderWithOffset> _keyReaders = new ConcurrentHashMap<>();

  // For each key buffer, this records how many rows have been written to that buffer. This is needed because
  // we need to be able to scan a key buffer for a particular doc id.
  private final ConcurrentHashMap<String, AtomicInteger> _keyBufferSize = new ConcurrentHashMap<>();
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
  public FixedByteSparseMapMutableForwardIndex(DataType storedType, int fixedLength,
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
    // TODO(ERICH): don't add any buffers until we start adding keys
    //addBuffer();
  }

  public FixedByteSparseMapMutableForwardIndex(DataType valueType, int numRowsPerChunk,
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
    // Get the buffer for the given key
    var writer = getWriterForKey(key);
    assert writer != null;
    writer.setDocAndInt(docId, value);
    var size = getBufferSizeForKey(key);
    size.incrementAndGet();
  }

  @Override
  public int getIntMap(int docId, String key) {
    var reader = _keyReaders.get(key);
    if(reader != null) {
      var size = getBufferSizeForKey(key);
      var maxRows = size.get();
      return reader.getReader().getIntValue(maxRows, docId);
    } else {
      // TODO(ERICH): if the key does not exist we should return the Null code
      return 0;
    }
  }

  private KeyValueWriterWithOffset getWriterForKey(String key) {
    var writer =  _keyWriters.get(key);

    if(writer == null) {
      // add a writer for this key
      addBufferForKey(key);
      writer = _keyWriters.get(key);
      assert writer != null;
    }

    return writer;
  }

  private AtomicInteger getBufferSizeForKey(String key) {
    var size = _keyBufferSize.get(key);
    assert size != null;
    return size;
  }

  @Override
  public void close()
      throws IOException {
    for (KeyValueWriterWithOffset writer : _keyWriters.values()) {
      writer.close();
    }
    for (KeyValueReaderWithOffset reader : _keyReaders.values()) {
      reader.close();
    }
  }

  private void addBufferForKey(String key) {
    LOGGER.info("Allocating {} bytes for: {}", _chunkSizeInBytes, _allocationContext);
    // NOTE: PinotDataBuffer is tracked in the PinotDataBufferMemoryManager. No need to track it inside the class.
    PinotDataBuffer buffer = _memoryManager.allocate(_chunkSizeInBytes, _allocationContext);
    final int keySize = 4;
    _keyWriters.put(
        key,
        new KeyValueWriterWithOffset(
            new FixedByteMapValueMultiColWriter(buffer, keySize, _valueSizeInBytes),
            _capacityInRows));

    _keyBufferSize.put(key, new AtomicInteger(0));

    _keyReaders.put(
        key,
        new KeyValueReaderWithOffset(
            new FixedByteMapValueMultiColReader(buffer, _numRowsPerChunk, keySize, _valueSizeInBytes),
            _capacityInRows));
    _capacityInRows += _numRowsPerChunk;
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

  private static class KeyValueWriterWithOffset implements Closeable {
    // TODO(ERICH): I assumed that within this class `row` is equivalent to `docId` in the outer class. Will rename to
    //  docId because that's what will be written in the tuple.
    final FixedByteMapValueMultiColWriter _writer;
    final int _startDocId;
    int _nextRow;

    private KeyValueWriterWithOffset(FixedByteMapValueMultiColWriter writer, int startDocId) {
      _writer = writer;
      _startDocId = startDocId;
      _nextRow = 0;
    }

    @Override
    public void close()
        throws IOException {
      _writer.close();
    }

    public void setDocAndInt(int docId, int value) {
      // Write the tuple of (docId, Value) to the index buffer

      /*
        The format for the buffer is:
        | docId (4 bytes) | value (4 bytes) | docId (4 bytes) | value (4 bytes) | ... |
       */
      _writer.setIntValue(_nextRow, docId, value);
      _nextRow++;
    }
  }

  /**
   * Helper class that encapsulates reader and global startRowId.
   *
   * For reading from the sparse map, this will have to scan the buffer for
   * any given docId.
   */
  private static class KeyValueReaderWithOffset implements Closeable {
    // TODO(ERICH): change rowId to docId to make it more accurately reflect the sparse data structure.
    //  - Move the creation of the FixedByteSVMultiColReader into this ctor so that it can make sure that it is configured for tuples
    final FixedByteMapValueMultiColReader _reader;
    final int _startDocId;

    private KeyValueReaderWithOffset(FixedByteMapValueMultiColReader reader, int startDocId) {
      _reader = reader;
      _startDocId = startDocId;
    }

    @Override
    public void close()
        throws IOException {
      _reader.close();
    }

    public int getInt(int docId) {
      // From start of buffer iterate through each docId, int value pair until the docId
      // specified is found
      // The Doc ID and Values are written as pairs in memory, so the loop will need to read the docId while skipping
      // over the value

      /*
        The format for the buffer is:
        | docId (4 bytes) | value (4 bytes) | docId (4 bytes) | value (4 bytes) | ... |
       */
      return 0;
    }

    public FixedByteMapValueMultiColReader getReader() {
      return _reader;
    }
  }
}
