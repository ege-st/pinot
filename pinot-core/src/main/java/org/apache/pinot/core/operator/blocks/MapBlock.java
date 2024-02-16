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
package org.apache.pinot.core.operator.blocks;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import org.apache.pinot.segment.local.segment.index.datasource.MutableMapDataSource;


/**
 * ProjectionBlock holds a column name to Block Map.
 * It provides DocIdSetBlock for a given column.
 */
public class MapBlock implements ValueBlock {
  private final MutableMapDataSource _mapDataSource;
  private final DataBlockCache _dataBlockCache;
  private final String _columnName;

  public MapBlock(String column, MutableMapDataSource ds, DataBlockCache dataBlockCache) {
    _columnName = column;
    _mapDataSource = ds;
    _dataBlockCache = dataBlockCache;
  }

  @Override
  public int getNumDocs() {
    return _dataBlockCache.getNumDocs();
  }

  @Override
  public int[] getDocIds() {
    return _dataBlockCache.getDocIds();
  }

  @Override
  public BlockValSet getBlockValueSet(ExpressionContext expression) {
    throw new UnsupportedOperationException();
    //assert expression.getType() == ExpressionContext.Type.IDENTIFIER;
    //return getBlockValueSet(expression.getIdentifier());
  }

  @Override
  public BlockValSet getBlockValueSet(String key) {
    return new ProjectionBlockValSet(_dataBlockCache, _columnName + "."  + key, _mapDataSource.getKey(key));
  }

  @Override
  public MapBlock getMap(String key) {
    // In the future when we support nested maps this will be able to return maps nested within maps.
    throw new UnsupportedOperationException("Attempting to get a Map Index from a Non-Map column");
  }
}
