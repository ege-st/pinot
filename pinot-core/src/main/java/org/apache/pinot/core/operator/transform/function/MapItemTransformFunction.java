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
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


/**
 * Evaluates myMap['foo']
 */
public class MapItemTransformFunction {
  public static class MapItemFunction extends BaseTransformFunction {
    public static final String FUNCTION_NAME = "map_item";
    String _column;
    String _key;
    TransformFunction _mapValue;
    TransformFunction _keyValue;

    @Override
    public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
      super.init(arguments, columnContextMap);
      // Should be exactly 2 arguments (map value expression and key expression
      if (arguments.size() != 2) {
        throw new IllegalArgumentException("Exactly 1 argument is required for Vector transform function");
      }

      // Check if the second operand (the key) is a string literal, if it is then we can directly construct the
      // MapDataSource which will pre-compute the Key ID.

      _mapValue = arguments.get(0);
      Preconditions.checkArgument(_mapValue instanceof IdentifierTransformFunction, "Map Item: Left operand"
          + "must be an identifier");
      _column = ((IdentifierTransformFunction) _mapValue).getColumnName();
      if (_column == null ) {
        throw new IllegalArgumentException("Map Item: left operand resolved to a null column name");
      }

      Preconditions.checkArgument(_keyValue instanceof LiteralTransformFunction, "Map Item: Left operand"
          + "must be a literal");
      _key = ((LiteralTransformFunction) arguments.get(1)).getStringLiteral();
      Preconditions.checkArgument(_key != null, "Map Item: Left operand"
          + "must be a string literal");
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
      return valueBlock.getMap(_column).getBlockValueSet(_key).getIntValuesSV();
    }
  }
}
