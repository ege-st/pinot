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

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 *
 * Generic utility class to read data from file. The data file consists of rows
 * and columns. The number of columns are fixed. Each column can have either
 * single value or multiple values. There are two basic types of methods to read
 * the data <br>
 * 1. &lt;TYPE&gt; getType(int row, int col) this is used for single value column <br>
 * 2. int getTYPEArray(int row, int col, TYPE[] array). The caller has to create
 * and initialize the array. The implementation will fill up the array. The
 * caller is responsible to ensure that the array is big enough to fit all the
 * values. The return value tells the number of values.<br>
 *
 *
 */
public class FixedByteMapValueMultiColReader implements Closeable {
  private final PinotDataBuffer _dataBuffer;
  private final int _numRows;
  private final int _keySize;
  private final int _keyOffset;
  private final int _columnSize;
  private final int _columnOffSet;
  private final int _rowSize;

  public FixedByteMapValueMultiColReader(PinotDataBuffer dataBuffer, int numRows, int keySize, int columnSize) {
    _dataBuffer = dataBuffer;
    _numRows = numRows;
    _keySize = keySize;
    _columnSize = columnSize;
    int numColumns = 1;
    _keyOffset = 0;
    _columnOffSet = _keyOffset + _keySize;
    _rowSize = _keySize + _columnSize;
  }

  /**
   * Computes the offset where the actual column data can be read
   *
   */
  private int computeKeyOffset(int row) {
    final int offset = row * _rowSize + _keyOffset;
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
  public int getIntValue(int row) {
    assert getColumnSize() == 4;
    final int offset = computeValueOffset(row);
    return _dataBuffer.getInt(offset);
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

  public void readIntValues(int[] rows, int startPos, int limit, int[] values, int outStartPos) {
    int endPos = startPos + limit;
    for (int iter = startPos; iter < endPos; iter++) {
      values[outStartPos++] = getIntValue(rows[iter]);
    }
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
