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
package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Interface for map index creator.
 */
public interface MapIndexCreator extends IndexCreator {
  public static final int VERSION_1 = 1;

  default void seal()
      throws IOException {
  }

  /**
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param dict This is ignored as the MapIndexCreator will manage the construction of dictionaries itself.
   */
  @Override
  default void add(@Nonnull Object value, int dict) throws IOException {
    Map<String, Object> mapValue = (Map<String, Object>) value;
    add(mapValue);
  }

  @Override
  default void add(@Nonnull Object[] values, int[] dictIds) {
    throw new UnsupportedOperationException("Array of Maps not supported yet");
  }

  void add(@Nonnull Map<String, Object> mapValue) throws IOException;

  /**
   * Returns {@code true} if the forward index for the given key is dictionary-encoded, {@code false} if it is raw.
   */
  boolean isDictionaryEncoded(String key);

  /**
   * Returns {@code true} if the forward index for the given key is for a single-value column, {@code false} if it is for a multi-value
   * column.
   */
  default boolean isSingleValue(String key) {
    // First version will not handle multivalue values in the map
    return false;
  }

  /**
   * Returns the data type of the values in the forward index for the given key. Returns {@link DataType#INT} for dictionary-encoded
   * forward index.
   */
  DataType getValueType(String key);
}
