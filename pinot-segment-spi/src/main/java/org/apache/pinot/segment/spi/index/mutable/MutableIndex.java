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

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;

/**
 * Implementations of this interface can be used to create indexes in realtime tables and at the same time to read them.
 *
 * This interface extends {@link IndexReader} as a marker interface and implementations are encouraged to extend the
 * index specific reader. For example, MutableForwardIndex should extend ForwardIndexReader. The Java typesystem is not
 * expressive enough to enforce this constraint.
 *
 * This interface explicitly do not extend {@link org.apache.pinot.segment.spi.index.IndexCreator} and usually
 * implementations should not implement that interface. As explained in their Javadoc, IndexCreators are designed to
 * add rows in docId order. MutableIndexes, on the other hand, can be modified adding indexes in any order and therefore
 * their {@link #add(Object, int, int)} methods include an extra parameter that indicates the docId of the value to add.
 */
public interface MutableIndex extends IndexReader {
  /**
   * Adds the given single value cell to the index.
   *
   * Unlike {@link org.apache.pinot.segment.spi.index.IndexCreator#add(Object, int)}, rows can be added in no
   * particular order, so the docId is required by this method.
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param dictId An optional dictionary value of the cell. If there is no dictionary, -1 is received
   * @param docId The document id of the given row. A non-negative value.
   */
  void add(@Nonnull Object value, int dictId, int docId);

  /**
   * Adds the given multi value cell to the index.
   *
   * Unlike {@link org.apache.pinot.segment.spi.index.IndexCreator#add(Object[], int[])}, rows can be added in no
   * particular order, so the docId is required by this method.
   *
   * @param values The nonnull value of the cell. In case the cell was actually null, an empty array is received instead
   * @param dictIds An optional array of dictionary values. If there is no dictionary, null is received.
   * @param docId The document id of the given row. A non-negative value.
   */
  void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId);

  /**
   * Adds the given map value cell to an index.  The dictionary ids are for encoding the Values (not the Keys).
   *
   * @param value
   * @param dictIds
   * @param docId
   */
  default void add(@Nonnull Map<String, Object> value, @Nullable int[] dictIds, int docId) {
    // TODO: Think of this as raw encoded, just pass in the string and let the index figure it out
    //   Have the forward and inverted indexes create their own dictionaries rather than the Mutable Segment
    throw new UnsupportedOperationException();
  }

  /**
   * Adds the given key value pair to the index for the given Document.
   *
   * @param key - the key to use under this map index
   * @param value - the value to associate with the key
   * @param dictId - dictId if using an dictionary encoded value
   * @param docId - the Document whose Map value is being added to.
   */
  default void add(@Nonnull String key, @Nonnull Object value, int dictId, int docId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Adds an entire map value to the index for a document.
   *
   * @param keys
   * @param values
   * @param dicIds
   * @param docId
   */
  default void add(@Nonnull String[] keys, @Nonnull Object[] values, @Nullable int[] dicIds, int docId) {
    throw new UnsupportedOperationException();
  }
}
