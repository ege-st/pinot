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
package org.apache.pinot.segment.local.io.reader.impl;

import java.io.Closeable;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;



/**
 *
 * A utility for reading the DocID to Value assignments of a specific Key.
 *
 * This format is used for dealing with Key major Map (or Dictionary) data storage. Where
 * at the top level a key is mapped to a buffer and in the buffer is the set of Doc Ids that
 * have a value for the Key and what that value is:
 *
 *    | Key |  (Doc Id, Value) |
 *    |-----------------------|
 *    | foo |  (2, 56)        |
 *    |     |  (5, 156)       |
 *    |     |  (15, 336)      |
 *    | bar |  (0, 1)         |
 *    |     |  (4, 10)        |
 *
 */
public class FixedByteMapValueMultiColReader implements Closeable {
  private final PinotDataBuffer _dataBuffer;
  private final int _numRows;
  private final int _docIdSize;
  private final int _docIdOffset;
  private final int _columnSize;
  private final int _columnOffSet;
  private final int _rowSize;

  public FixedByteMapValueMultiColReader(PinotDataBuffer dataBuffer, int numRows, int docIdSize, int columnSize) {
    _dataBuffer = dataBuffer;
    _numRows = numRows;
    _docIdSize = docIdSize;
    _columnSize = columnSize;
    _docIdOffset = 0;
    _columnOffSet = _docIdOffset + _docIdSize;
    _rowSize = _docIdSize + _columnSize;
  }

  /**
   * Computes the offset where the actual column data can be read
   *
   */
  private int computeDocIdOffset(int row) {
    final int offset = row * _rowSize + _docIdOffset;
    return offset;
  }

  private int computeValueOffset(int row) {
    final int offset = row * _rowSize + _columnOffSet;
    return offset;
  }

  /**
   *
   * @param row
   * @return
   */
  public int getIntValueAt(int row) {
    assert getColumnSize() == 4;
    final int offset = computeValueOffset(row);
    return _dataBuffer.getInt(offset);
  }

  public int getIntValue(int maxRow, int docId) {
    assert getColumnSize() == 4;
    // Scan the buffer to find the given key
    // TODO(ERICH): how to know when to stop scanning?
    for(int row = 0; row < maxRow; row++){
      final int docIdOffset = computeDocIdOffset(row);
      final int currentDocId = _dataBuffer.getInt(docIdOffset);
      if(currentDocId == docId) {
        // The DocID has a value for this key
        return getIntValueAt(row);
      }
    }

    // The DocId does not have a value for this key so return Null

    // Return None if the docId has no value for this key
    // TODO(ERICH): what is done semantically if a  docId does not have the key? Treat as Null and then what?
    //   Must require null value support to be enabled.
    return 0;
  }

  public int getNumberOfRows() {
    return _numRows;
  }

  public int getColumnSize() {
    return _columnSize;
  }

  public boolean open() {
    return false;
  }

  public void readIntValuesAt(int[] rows, int startPos, int limit, int[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getIntValueAt(rows[iter]);
    }
  }

  public void readIntValues(int maxRow, int[] docIds, int startPos, int limit, int[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getIntValue(maxRow, docIds[iter]);
    }
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
