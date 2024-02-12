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
public class MapIndexConfig extends IndexConfig {
  public static final MapIndexConfig DISABLED = new MapIndexConfig(true);

  private Set<String> _denseKeys;
  private int _maxDenseKeys = 20;
  private int _maxKeysPerDoc = 10;

  public MapIndexConfig() {
    super(false);
  }

  public MapIndexConfig(Boolean disabled) {
    super(disabled);
  }

  @JsonCreator
  public MapIndexConfig(@JsonProperty("disabled") Boolean disabled,
      @JsonProperty("maxDenseKeys") int maxDenseKeys,
      @JsonProperty("denseKeys") @Nullable Set<String> denseKeys,
      @JsonProperty("maxKeysPerDoc") int maxKeysPerDoc) {
    super(disabled);
    _denseKeys = denseKeys;
    _maxDenseKeys = maxDenseKeys;
    _maxKeysPerDoc = maxKeysPerDoc;
  }

  public int getMaxKeysPerDoc() {
    return _maxKeysPerDoc;
  }

  public void setMaxKeysPerDoc(int maxKeysPerDoc) {
    _maxKeysPerDoc = maxKeysPerDoc;
  }

  public int getMaxDenseKeys() {
    return _maxKeysPerDoc;
  }

  public void setMaxDenseKeys(int maxDenseKeys) {
    Preconditions.checkArgument(_denseKeys == null || _maxDenseKeys >= _denseKeys.size(),
        "maxDenseKeys must be greater than or equal to the size of denseKeys");
    _maxDenseKeys = maxDenseKeys;
  }

  @Nullable
  public Set<String> getDenseKeys() {
    return _denseKeys;
  }

  public void setDenseKeys(@Nullable Set<String> denseKeys) {
    Preconditions.checkArgument(_denseKeys == null || _denseKeys.size() <= _maxDenseKeys,
        "The size of denseKeys must be less than or equal to the value of maxDenseKeys");
    _denseKeys = denseKeys;
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
    MapIndexConfig config = (MapIndexConfig) o;
    return this._maxDenseKeys == config._maxDenseKeys
        && this._maxKeysPerDoc == config._maxKeysPerDoc
        && this._denseKeys.equals(config._denseKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _maxDenseKeys, _maxKeysPerDoc, _denseKeys);
  }
}
