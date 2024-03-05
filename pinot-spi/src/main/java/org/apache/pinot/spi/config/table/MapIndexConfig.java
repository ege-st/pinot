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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Configs related to the MAP index:
 */
public class MapIndexConfig extends IndexConfig {
  public static final MapIndexConfig DISABLED = new MapIndexConfig(true);

  private int _maxKeys = 100;
  private List<String> _denseKeys;

  public MapIndexConfig() {
    super(false);
  }

  public MapIndexConfig(Boolean disabled) {
    super(disabled);
  }

  @JsonCreator
  public MapIndexConfig(@JsonProperty("disabled") Boolean disabled,
      @JsonProperty("maxKeys") int maxKeys,
      @JsonProperty("denseKeys") List<String> denseKeys)  {
    super(disabled);
    _maxKeys = maxKeys;
    _denseKeys = denseKeys;
  }

  public int getMaxKeys() {
    return _maxKeys;
  }

  public void setMaxLevels(int maxKeys) {
    _maxKeys = maxKeys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MapIndexConfig config = (MapIndexConfig) o;
    return _maxKeys == config._maxKeys;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _maxKeys);
  }
}
