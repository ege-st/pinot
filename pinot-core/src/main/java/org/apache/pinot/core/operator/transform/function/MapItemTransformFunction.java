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
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;


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
    Dictionary _dictionary;
    private TransformResultMetadata _resultMetadata;

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

      _keyValue = arguments.get(1);
      Preconditions.checkArgument(_keyValue instanceof LiteralTransformFunction, "Map Item: Right operand"
          + "must be a literal");
      _key = ((LiteralTransformFunction) arguments.get(1)).getStringLiteral();
      Preconditions.checkArgument(_key != null, "Map Item: Right operand"
          + "must be a string literal");

      _dictionary = columnContextMap.get(_column).getDictionary();
      _resultMetadata =
          new TransformResultMetadata(columnContextMap.get(_column).getDataType(), columnContextMap.get(_column).isSingleValue(),
              _dictionary != null);
    }

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    public TransformResultMetadata getResultMetadata() {
      return new TransformResultMetadata(_resultMetadata.getDataType().getStoredType(), true, _resultMetadata.hasDictionary());
    }

    @Override
    public Dictionary getDictionary() {
      return _dictionary;
    }

    @Override
    public int[] transformToDictIdsSV(ValueBlock valueBlock) {
      return transformToIntValuesSV(valueBlock);
    }

    @Override
    public int[] transformToIntValuesSV(ValueBlock valueBlock) {
      int length = valueBlock.getNumDocs();
      initIntValuesSV(length);
      var mapCol = valueBlock.getMap(_column);
      return valueBlock.getBlockValueSet(_column).getIntValuesMap(_key);
      //var mapBlock = mapCol.getBlockValueSet(_key);
      //return mapBlock.getIntValuesSV();
    }
  }
}
