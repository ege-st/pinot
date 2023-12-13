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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.pinot.segment.local.io.reader.impl.FixedByteMapValueMultiColReader;
import org.apache.pinot.segment.local.io.reader.impl.FixedByteSingleValueMultiColReader;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteMapValueMultiColWriter;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteSingleValueMultiColWriter;
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
// TODO: Optimize it
public class FixedByteSparseMapMutableForwardIndex implements MutableForwardIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSparseMapMutableForwardIndex.class);

  // For single writer multiple readers setup, use ArrayList for writer and CopyOnWriteArrayList for reader
  private final List<KeyValueWriterWithOffset> _writers = new ArrayList<>();
  private final List<KeyValueReaderWithOffset> _readers = new CopyOnWriteArrayList<>();

  private final DataType _storedType;
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
    _storedType = storedType;
    if (!storedType.isFixedWidth()) {
      Preconditions.checkState(fixedLength > 0, "Fixed length must be provided for type: %s", storedType);
      _valueSizeInBytes = fixedLength;
    } else {
      _valueSizeInBytes = storedType.size();
    }
    _numRowsPerChunk = numRowsPerChunk;
    _chunkSizeInBytes = numRowsPerChunk * (long)_valueSizeInBytes;
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    addBuffer();
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

  // TODO(Erich): add isMapValue() to interface

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

  @Override
  public int getDictId(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getInt(docId);
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer) {
    /*
     * TODO
     * If we assume that the document ids are sorted, then we can write logic to move values from one reader
     * at a time, identifying the rows in sequence that belong to the same block. This logic is more complex, but may
     * perform better in the sorted case.
     *
     * An alternative is to not have multiple _dataBuffers, but just copy the values from one buffer to next as we
     * increase the number of rows.
     */
    if (_readers.size() == 1) {
      _readers.get(0).getReader().readIntValues(docIds, 0, length, dictIdBuffer, 0);
    } else {
      for (int i = 0; i < length; i++) {
        int docId = docIds[i];
        dictIdBuffer[i] = _readers.get(getBufferId(docId)).getInt(docId);
      }
    }
  }

  @Override
  public int getInt(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getInt(docId);
  }

  private int getBufferId(int row) {
    return row / _numRowsPerChunk;
  }

  @Override
  public void setDictId(int docId, int dictId) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setDocAndInt(docId, dictId);
  }

  @Override
  public void setInt(int docId, int value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setDocAndInt(docId, value);
  }

  private KeyValueWriterWithOffset getWriterForRow(int row) {
    return _writers.get(getBufferId(row));
  }

  @Override
  public void close()
      throws IOException {
    for (KeyValueWriterWithOffset writer : _writers) {
      writer.close();
    }
    for (KeyValueReaderWithOffset reader : _readers) {
      reader.close();
    }
  }

  private void addBuffer() {
    LOGGER.info("Allocating {} bytes for: {}", _chunkSizeInBytes, _allocationContext);
    // NOTE: PinotDataBuffer is tracked in the PinotDataBufferMemoryManager. No need to track it inside the class.
    PinotDataBuffer buffer = _memoryManager.allocate(_chunkSizeInBytes, _allocationContext);
    final int keySize = 4;
    _writers.add(
        new KeyValueWriterWithOffset(new FixedByteMapValueMultiColWriter(buffer, keySize, _valueSizeInBytes),
            _capacityInRows));
    _readers.add(new KeyValueReaderWithOffset(
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
        addBuffer();
      }
    }
  }

  private static class KeyValueWriterWithOffset implements Closeable {
    // TODO(ERICH): I assumed that within this class `row` is equivalent to `docId` in the outer class. Will rename to
    //  docId because that's what will be written in the tuple.
    final FixedByteMapValueMultiColWriter _writer;
    final int _startDocId;

    private KeyValueWriterWithOffset(FixedByteMapValueMultiColWriter writer, int startDocId) {
      _writer = writer;
      _startDocId = startDocId;
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
