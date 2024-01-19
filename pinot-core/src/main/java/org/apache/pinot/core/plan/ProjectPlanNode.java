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
package org.apache.pinot.core.plan;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.ProjectionOperatorUtils;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;


/**
 * The <code>ProjectPlanNode</code> provides the execution plan for fetching column values on a single segment.
 */
public class ProjectPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final Collection<ExpressionContext> _expressions;
  private final int _maxDocsPerCall;
  private final BaseFilterOperator _filterOperator;

  public ProjectPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      Collection<ExpressionContext> expressions, int maxDocsPerCall, @Nullable BaseFilterOperator filterOperator) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    _expressions = expressions;
    _maxDocsPerCall = maxDocsPerCall;
    _filterOperator = filterOperator;
  }

  public ProjectPlanNode(IndexSegment indexSegment, QueryContext queryContext,
      Collection<ExpressionContext> expressions, int maxDocsPerCall) {
    this(indexSegment, queryContext, expressions, maxDocsPerCall, null);
  }

  @Override
  public BaseProjectOperator<?> run() {
    Set<String> projectionColumns = new HashSet<>();
    Set<ImmutablePair<String, String>> mapItemColumns = new HashSet<>();

    boolean hasNonIdentifierExpression = false;
    for (ExpressionContext expression : _expressions) {
      if (expression.getType() == ExpressionContext.Type.FUNCTION
          && expression.getFunction().getFunctionName().equals("item")) {
        // This is an item operation on a map column
        String columnOp = expression.getFunction().getArguments().get(0).toString();
        String key = expression.getFunction().getArguments().get(1).toString();
        mapItemColumns.add(new ImmutablePair<>(columnOp, key));
      } else {
        expression.getColumns(projectionColumns);
      }

      if (expression.getType() != ExpressionContext.Type.IDENTIFIER) {
        hasNonIdentifierExpression = true;
      }
    }
    Map<String, DataSource> dataSourceMap = new HashMap<>(HashUtil.getHashMapCapacity(projectionColumns.size()));

    // TODO(ERICH): if the expression type is an item operator with map column then create a Map DataSource and pass the key
    projectionColumns.forEach(column -> dataSourceMap.put(column, _indexSegment.getDataSource(column)));
    mapItemColumns.forEach(operands ->
        dataSourceMap.put(operands.left, _indexSegment.getDataSource(operands.left, operands.right)));

    // NOTE: Skip creating DocIdSetOperator when maxDocsPerCall is 0 (for selection query with LIMIT 0)
    DocIdSetOperator docIdSetOperator =
        _maxDocsPerCall > 0 ? new DocIdSetPlanNode(_indexSegment, _queryContext, _maxDocsPerCall, _filterOperator).run()
            : null;
    ProjectionOperator projectionOperator =
        ProjectionOperatorUtils.getProjectionOperator(dataSourceMap, docIdSetOperator);
    return hasNonIdentifierExpression ? new TransformOperator(_queryContext, projectionOperator, _expressions)
        : projectionOperator;
  }
}
