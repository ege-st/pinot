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

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Interface for map index creator.
 */
public interface MapIndexCreator extends IndexCreator {
  default void seal()
      throws IOException {
  }

  /**
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param dict This is ignored as the MapIndexCreator will manage the construction of dictionaries itself.
   */
  @Override
  default void add(@Nonnull Object value, int dict) {
    Map<String, Object> mapValue = (Map<String, Object>) value;
    add(mapValue);
  }

  @Override
  default void add(@Nonnull Object[] values, int[] dictIds) {
    throw new UnsupportedOperationException("Array of Maps not supported yet");
  }

  default void add(@Nonnull Map<String, Object> mapValue) {
    // Iterate over each KV pair in this map and add to the underlying index
  }

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

  /**
   * SINGLE-VALUE COLUMN RAW INDEX APIs
   */

  /**
   * Writes the next INT type single-value into the forward index for the given key.
   *
   * @param value Value to write
   */
  default void putInt(String key, int value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next LONG type single-value into the forward index for the given key.
   *
   * @param value Value to write
   */
  default void putLong(String key, long value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next FLOAT type single-value into the forward index for the given key.
   *
   * @param value Value to write
   */
  default void putFloat(String key, float value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next DOUBLE type single-value into the forward index for the given key.
   *
   * @param value Value to write
   */
  default void putDouble(String key, double value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next BIG_DECIMAL type single-value into the forward index for the given key.
   *
   * @param value Value to write
   */
  default void putBigDecimal(String key, BigDecimal value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next STRING type single-value into the forward index for the given key.
   *
   * @param value Value to write
   */
  default void putString(String key, String value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next BYTES type single-value into the forward index for the given key.
   *
   * @param value Value to write
   */
  default void putBytes(String key, byte[] value) {
    throw new UnsupportedOperationException();
  }

  /**
   * MULTI-VALUE COLUMN RAW INDEX APIs
   * TODO: Not supported yet
   */

  /**
   * Writes the next INT type multi-value into the forward index for the given key.
   *
   * @param values Values to write
   */
  default void putIntMV(String key, int[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next LONG type multi-value into the forward index for the given key
   *
   * @param values Values to write
   */
  default void putLongMV(String key, long[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next FLOAT type multi-value into the forward index for the given key.
   *
   * @param values Values to write
   */
  default void putFloatMV(String key, float[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next DOUBLE type multi-value into the forward index for the given key.
   *
   * @param values Values to write
   */
  default void putDoubleMV(String key, double[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next STRING type multi-value into the forward index for the given key.
   *
   * @param values Values to write
   */
  default void putStringMV(String key, String[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next byte[] type multi-value into the forward index for the given key.
   *
   * @param values Values to write
   */
  default void putBytesMV(String key, byte[][] values) {
    throw new UnsupportedOperationException();
  }
}
