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
import java.io.UncheckedIOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;


public interface MutableMapIndex extends MapIndexReader, MutableIndex {
  @Override
  default void add(@Nonnull Object value, int dictId, int docId) {
    throw new UnsupportedOperationException("Map Index support not yet enabled");
  }

  @Override
  default void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
    throw new UnsupportedOperationException("Mutable Map indexes are not supported for multi-valued columns");
  }

  /**
   * Given a key this will return an IndexReader that allows you to read the values bound to the given key just
   * as if it were a regular column of values.
   *
   * @param key A key within this map column
   * @return an IndexReader that allows you to interact with the value each document has bound to <i>key</i> just as if
   * <i>key</i> were a forward index.
   */
  @Override
  default MutableIndex getKeyReader(String key) {
    throw new UnsupportedOperationException();
  }
}
