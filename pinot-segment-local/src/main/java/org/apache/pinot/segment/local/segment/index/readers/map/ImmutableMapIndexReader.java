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
package org.apache.pinot.segment.local.segment.index.readers.map;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import org.apache.pinot.segment.local.io.util.VarLengthValueReader;
import org.apache.pinot.segment.spi.index.creator.MapIndexCreator;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Reader for map index.
 *
 * The Header for the Map Index
 * - Information that is needed:
 * 1. The number of dense columns in the set
 * 2. The number of sparse columns in the set
 * 3. The offset of each column within the data buffer
 * 4. The type of each column
 * 5. The index type of each column
 * 6. The name of the key for each dense column
 *
 * Header Layout
 * | Version Number
 * | Number of Dense Keys
 * | Offset of Dense Key Metadata
 * | Offset of Sparse Key Metadata
 * | Number of Sparse Indexes
 * | (Offset, Dense Key Name, Type)  Or do we store the metadata for each Keylumn
 * | .... |
 * | (Sparse column offset, sparse column metadata)
 * | .... |
 * | ...Actual Data... |
 *
 * Quesitons:
 * 1. How can I a read a forward index from this data buffer?
 * 2. How do I read index metadata from this buffer?
 */
public class ImmutableMapIndexReader implements MapIndexReader {
  // NOTE: Use long type for _numDocs to comply with the RoaringBitmap APIs.
  private final long _numDocs;
  private final int _version;
  protected final PinotDataBuffer _dataBuffer;
  private final int _denseKeyStringNumBytesPerValue;
  private final byte[] _columnNameBuffer;
  private final HashMap<String, Integer> _denseKeyOffsets;

  public ImmutableMapIndexReader(PinotDataBuffer dataBuffer, int numDocs) {
    _numDocs = numDocs;
    _version = dataBuffer.getInt(0);
    Preconditions.checkState(_version == MapIndexCreator.VERSION_1,
        "Unsupported map index version: %s.  Valid versions are {}", _version, MapIndexCreator.VERSION_1);
    _dataBuffer = dataBuffer;
    _denseKeyStringNumBytesPerValue = 4;
    _columnNameBuffer = new byte[_denseKeyStringNumBytesPerValue];
    _denseKeyOffsets = new HashMap<>();
  }

  private void readKeyOffsets() {
    // Read the number of dense keys
    // Read the keys and their offsets
    final int numDenseKeyOffset = 4;
    final int numDenseKeys = _dataBuffer.getInt(numDenseKeyOffset);

    final VarLengthValueReader reader = new VarLengthValueReader(_dataBuffer);
    int offset = numDenseKeyOffset + 4;
    for (int i = 0; i < numDenseKeys; i++) {
      int columnOffset = reader.getInt(offset);
      offset += 4;
      String columnName = reader.getPaddedString(numDenseKeyOffset, _denseKeyStringNumBytesPerValue, _columnNameBuffer);
      offset += _denseKeyStringNumBytesPerValue;

      _denseKeyOffsets.put(columnName, columnOffset);
    }
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }

  @Override
  public boolean isDictionaryEncoded(String key) {
    return false;
  }

  @Override
  public FieldSpec.DataType getStoredType(String key) {
    return null;
  }
}
