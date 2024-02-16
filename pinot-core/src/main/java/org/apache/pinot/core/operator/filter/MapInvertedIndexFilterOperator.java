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
package org.apache.pinot.core.operator.filter;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.local.realtime.impl.map.MutableMapInvertedIndex;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.trace.FilterType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Filter operator for JSON_MATCH. E.g. SELECT ... WHERE JSON_MATCH(column_name, filter_string)
 */
public class MapInvertedIndexFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_JSON_INDEX";

  private final MutableMapInvertedIndex _mii;
  private final String _key;
  private final String _value;
  private final Dictionary _dictionary;

  public MapInvertedIndexFilterOperator( MutableMapInvertedIndex mii, String key, Predicate predicate, Dictionary dictionary,
      int numDocs) {
    super(numDocs, false);
    _mii = mii;
    _key = key;
    _dictionary = dictionary;
    if (predicate instanceof EqPredicate) {
      final String value = ((EqPredicate) predicate).getValue();
      if (_dictionary != null) {
        _value = String.valueOf(_dictionary.indexOf(value));
      } else {
        _value = value;
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  protected BlockDocIdSet getTrues() {
    ImmutableRoaringBitmap bitmap = _mii.getDocIdsWithKeyValue(_key, _value);
    record(bitmap);
    return new BitmapDocIdSet(bitmap, _numDocs);
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    return _mii.getDocIdsWithKeyValue(_key, _value).getCardinality();
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    return new BitmapCollection(_numDocs, false, _mii.getDocIdsWithKeyValue(_key, _value));
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(EXPLAIN_NAME).append("(indexLookUp:json_index");
    stringBuilder.append(",operator:").append("EQ");
    stringBuilder.append(",predicate:").append("STRING");
    return stringBuilder.append(')').toString();
  }

  private void record(ImmutableRoaringBitmap bitmap) {
    InvocationRecording recording = Tracing.activeRecording();
    if (recording.isEnabled()) {
    }
  }
}
