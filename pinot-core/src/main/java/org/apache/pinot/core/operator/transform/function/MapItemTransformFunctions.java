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
package org.apache.pinot.core.operator.transform.function;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.scalar.MapFunctions;
import org.apache.pinot.common.function.scalar.VectorFunctions;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


public class MapItemTransformFunctions {

  public static class MapItemFunction extends BaseTransformFunction {
    public static final String FUNCTION_NAME = "item";
    TransformFunction _mapValue;
    TransformFunction _keyValue;

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
      super.init(arguments, columnContextMap);
      // TODO:
      //   - First operand is the map value expression
      //   - Second operand is the key

      // Should be exactly 2 arguments (map value expression and key expression
      if (arguments.size() != 2) {
        throw new IllegalArgumentException("Exactly 1 argument is required for Vector transform function");
      }

      _mapValue = arguments.get(0);
      Preconditions.checkArgument(!_mapValue.getResultMetadata().isSingleValue() && _mapValue.getResultMetadata().isMapValue(),
          "Argument must be multi-valued float vector for vector distance transform function: %s", getName());

      _keyValue = arguments.get(1);
      Preconditions.checkArgument(_keyValue.getResultMetadata().isSingleValue() && !_mapValue.getResultMetadata().isMapValue(),
          "Argument must be a single valued String for map item transform function: %s", getName());
    }

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return INT_SV_NO_DICTIONARY_METADATA;
    }

    @Override
    public int[] transformToIntValuesSV(ValueBlock valueBlock) {
      int length = valueBlock.getNumDocs();
      initIntValuesSV(length);
      // TODO: do I need a transformToIntValuesMap method?

      // Evaluate the expression that will resolve to the Map value
      Map<String, Integer>[] maps = _mapValue.transformToIntValuesMap(valueBlock);

      // Evalulate the expression that will resolve to the key used to look up a map
      String[] keys = _keyValue.transformToStringValuesSV(valueBlock);

      // Check that both blocks have the same length
      assert maps.length == keys.length;
      assert maps.length == length;

      for (int i = 0; i < length; i++) {
        // Resolve teh expression that looks up a key in a map and resolves to the
        // value bound to that key or to Null.
        _intValuesSV[i] = MapFunctions.mapElementForKey(maps[i], keys[i]);
      }
      throw new UnsupportedOperationException();
    }
  }
}
