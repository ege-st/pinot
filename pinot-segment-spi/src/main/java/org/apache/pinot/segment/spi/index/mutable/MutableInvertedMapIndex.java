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
package org.apache.pinot.segment.spi.index.mutable;

import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.InvertedMapIndexReader;


/**
 * An inverted index on a map column.  This allows for looks ups by key and by key and value on a map column.
 */
public interface MutableInvertedMapIndex extends InvertedMapIndexReader, MutableIndex {
  /**
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param dictId An optional dictionary value of the cell. If there is no dictionary, -1 is received
   * @param docId The document id of the given row. A non-negative value.
   */
  @Override
  default void add(@Nonnull Object value, int dictId, int docId) {
    // TODO: This doesn't work because how do I get codes for keys and values?  Maybe not worry about that in the first
    //   version?
    throw new UnsupportedOperationException();
  }

  @Override
  default void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
    throw new UnsupportedOperationException("Mutable Inverted Map indexes are not supported for multi-valued columns");
  }

  /**
   * Index a
   */
  void add(int[] keyIds, int[] dictIds)
      throws IOException;
}
