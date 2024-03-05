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
import org.apache.pinot.segment.spi.index.creator.MapIndexCreator;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Reader for json index.
 */
public class ImmutableMapIndexReader implements MapIndexReader {
  // NOTE: Use long type for _numDocs to comply with the RoaringBitmap APIs.
  private final long _numDocs;
  private final int _version;
  protected final PinotDataBuffer _dataBuffer;

  public ImmutableMapIndexReader(PinotDataBuffer dataBuffer, int numDocs) {
    _numDocs = numDocs;
    _version = dataBuffer.getInt(0);
    Preconditions.checkState(_version == MapIndexCreator.VERSION_1,
        "Unsupported map index version: %s.  Valid versions are {}", _version, MapIndexCreator.VERSION_1);
    _dataBuffer = dataBuffer;
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
