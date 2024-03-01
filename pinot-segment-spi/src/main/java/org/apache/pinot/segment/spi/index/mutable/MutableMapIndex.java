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
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;


/**
 * Implementations of this interface can be used to represent indexes that store dynamically typed map values.
 */
public interface MutableMapIndex extends MapIndexReader {

  /**
   * Adds the given single value cell to the index.
   *
   * Unlike {@link org.apache.pinot.segment.spi.index.IndexCreator#add(Object, int)}, rows can be added in no
   * particular order, so the docId is required by this method.
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param docId The document id of the given row. A non-negative value.
   */
  void add(Map<String, Object> value, int docId);

  /**
   * Adds the given multi value cell to the index.
   *
   * Unlike {@link org.apache.pinot.segment.spi.index.IndexCreator#add(Object[], int[])}, rows can be added in no
   * particular order, so the docId is required by this method.
   *
   * @param values The nonnull value of the cell. In case the cell was actually null, an empty array is received instead
   * @param docIds The document id of the given row. A non-negative value.
   */
  void add(Map<String, Object>[] values, int[] docIds);
}
