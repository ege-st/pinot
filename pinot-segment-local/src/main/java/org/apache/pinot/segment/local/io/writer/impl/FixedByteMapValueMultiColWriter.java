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
package org.apache.pinot.segment.local.io.writer.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public class FixedByteMapValueMultiColWriter implements Closeable {
  private final int _docIdOffset;
  private final int _columnOffset;
  private final int _rowSizeInBytes;
  private final PinotDataBuffer _dataBuffer;
  private final boolean _shouldCloseDataBuffer;

  public FixedByteMapValueMultiColWriter(File file, int rows, int docIdSize, int columnSize)
      throws IOException {
    _docIdOffset = 0;
    _columnOffset = _docIdOffset + docIdSize;
    _rowSizeInBytes = docIdSize + columnSize;
    int totalSize = _rowSizeInBytes * rows;
    // Backward-compatible: index file is always big-endian
    _dataBuffer = PinotDataBuffer.mapFile(file, false, 0, totalSize, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
    _shouldCloseDataBuffer = true;
  }

  public FixedByteMapValueMultiColWriter(PinotDataBuffer dataBuffer, int keySize, int columnSize) {
    _docIdOffset = 0;
    _columnOffset = _docIdOffset + keySize;
    _rowSizeInBytes = keySize + columnSize;
    _dataBuffer = dataBuffer;
    // For passed in PinotDataBuffer, the caller is responsible for closing the PinotDataBuffer.
    _shouldCloseDataBuffer = false;
  }

  @Override
  public void close()
      throws IOException {
    if (_shouldCloseDataBuffer) {
      _dataBuffer.close();
    }
  }

  public boolean open() {
    return true;
  }

  public void setIntValue(int row, int docId, int i) {
    int docIdOffset = _rowSizeInBytes * row + _docIdOffset;
    int valueOffset = _rowSizeInBytes * row + _columnOffset;
    _dataBuffer.putInt(docIdOffset, docId);
    _dataBuffer.putInt(valueOffset, i);
  }
}
