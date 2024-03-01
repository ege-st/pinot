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
package org.apache.pinot.segment.local.segment.creator.impl.map;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.MAP_DENSE_INDEX_FILE_EXTENSION;
import static org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Dense Map Index Creator. This creates <i>Dense</i> indexes for keys. It does this by using existing indexes
 * for any key that is stored in the dense index creator.
 */
public final class DenseMapIndexCreator implements org.apache.pinot.segment.spi.index.creator.MapIndexCreator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DenseMapIndexCreator.class);

  //This will dump the content of temp buffers and ranges
  private static final boolean TRACE = false;

  public static final int VERSION = 1;


  //output file which will hold the range index
  private final File _mapIndexFile;

  //File where the input values will be stored. This is a temp file that will be deleted at the end
  //pinot data buffer MMapped - maps the content of _tempValueBufferFile
  private PinotDataBuffer _tempValueBuffer;
  //a simple wrapper over _tempValueBuffer to make it easy to read/write any Number (INT,LONG, FLOAT, DOUBLE)

  //pinot data buffer MMapped - maps the content of _tempDocIdBufferFile
  private PinotDataBuffer _docIdValueBuffer;
  //a simple wrapper over _docIdValueBuffer to make it easy to read/write any INT

  private final int _numValues;
  private int _nextDocId;
  private int _nextValueId;
  private final DataType _valueType;

  /**
   *
   * @param indexDir destination of the range index file
   * @param fieldSpec fieldspec of the column to generate the range index
   * @param valueType DataType of the column, INT if dictionary encoded, or INT, FLOAT, LONG, DOUBLE for raw encoded
   * @param numDocs total number of documents
   * @throws IOException
   */
  public DenseMapIndexCreator(File indexDir, FieldSpec fieldSpec, DataType valueType, int numDocs)
      throws IOException {
    Preconditions.checkArgument(fieldSpec.getDataType() == DataType.MAP,
        "Map Index requires the data type to be MAP.");
    Preconditions.checkArgument(fieldSpec.isSingleValueField(),
        "Map Index must be marked as a single value field.");

    _valueType = valueType;
    String columnName = fieldSpec.getName();
    _mapIndexFile = new File(indexDir, columnName + MAP_DENSE_INDEX_FILE_EXTENSION);
    _numValues = numDocs;
  }

  @Override
  public boolean isDictionaryEncoded(String key) {
    return false;
  }

  @Override
  public DataType getValueType(String key) {
    return null;
  }

  @Override
  public void putInt(String key, int value) {
    throw new UnsupportedOperationException("Do stuff");
  }

  @Override
  public void putString(String key, String value) {
    throw new UnsupportedOperationException("Do stuff");
  }

  public void close()
      throws IOException {

  }
}
