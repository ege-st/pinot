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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Configs related to the Map index:
 * - denseKeys: specify which keys are dense (keys which are not in this list are added to the sparse index)
 * - maxKeysPerDoc: specify the maximum number of keys a document can have
 * - maxDenseKeys: specify the maximum number of keys which can be dense (must be >= len(denseKeys))
 */
public class MapInvertedIndexConfig extends IndexConfig {
  public static final MapInvertedIndexConfig DISABLED = new MapInvertedIndexConfig(true);
  private int _maxEntries = 100_000;

  public MapInvertedIndexConfig() {
    super(false);
  }

  public MapInvertedIndexConfig(Boolean disabled) {
    super(disabled);
  }

  @JsonCreator
  public MapInvertedIndexConfig(@JsonProperty("disabled") Boolean disabled,
      @JsonProperty("maxEntries") int maxEntries) {
    super(disabled);
    _maxEntries = maxEntries;
  }

  public int getMaxEntries() {
    return _maxEntries;
  }

  public void setMaxEntries(int maxEntries) {
    _maxEntries = maxEntries;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      // Same object so they must be equal
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      // Different classes so they can't be equal
      return false;
    }
    if (!super.equals(o)) {
      // The values in their parent are different, so they must be different
      return false;
    }

    // Test the fields specific to this class for equality
    MapInvertedIndexConfig config = (MapInvertedIndexConfig) o;
    return this._maxEntries == config._maxEntries;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _maxEntries);
  }
}
