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
package org.apache.pinot.segment.local.segment.index.datasource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code MutableDataSource} class is the data source for a map type column in the mutable segment.
 */
@SuppressWarnings("rawtypes")
public class MutableMapDataSource extends BaseDataSource {

  public MutableMapDataSource(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry, int cardinality,
      @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions, @Nullable Comparable minValue,
      @Nullable Comparable maxValue, Map<IndexType, MutableIndex> mutableIndexes,
      int maxRowLengthInBytes) {
    super(new MutableMapDataSourceMetadata(fieldSpec, numDocs, numValues, maxNumValuesPerMVEntry, cardinality,
            partitionFunction, partitions, minValue, maxValue, maxRowLengthInBytes),
        new ColumnIndexContainer.FromMap.Builder()
            .withAll(mutableIndexes)
            .build());
  }

  public DataSource getKey(String key) {
    MutableMapDataSourceMetadata md = (MutableMapDataSourceMetadata) getDataSourceMetadata();
    MapIndexReader mpi = getMapIndex();
    assert mpi != null;
    MutableIndex keyReader = (MutableIndex) mpi.getKeyReader(key);

    // TODO: Delete if MutableDataSource works
    //ColumnIndexContainer cic = new ColumnIndexContainer.FromMap.Builder()
        //.with(StandardIndexes.forward(), keyReader).build();
    //ColumnMetadataImpl cmd = createColumnMetadata(md);
    FieldSpec keyFS = createKeyFieldSpec();
    return new MutableDataSource(
        keyFS,
        md._numDocs,
        md._numValues,
        md._maxNumValuesPerMVEntry,
        md._cardinality,
        md._partitionFunction,
        md._partitions,
        md._minValue,
        md._maxValue,
        Map.of(StandardIndexes.forward(), keyReader),
        null,
        null,
        md._maxRowLengthInBytes
    );
  }

  private ColumnMetadataImpl createColumnMetadata(MutableMapDataSourceMetadata md) {
    // TODO: Delete this if mutable data source works
    FieldSpec keyFS = createKeyFieldSpec();
    return ColumnMetadataImpl.builder()
        .setHasDictionary(false)
        .setCardinality(md._cardinality)
        .setMaxNumberOfMultiValues(md._maxNumValuesPerMVEntry)
        .setColumnMaxLength(md._maxRowLengthInBytes)
        .setFieldSpec(keyFS)
        .setMaxValue(md._maxValue)
        .setMinValue(md._minValue)
        .setTotalDocs(md._numDocs)
        .setPartitionFunction(md._partitionFunction)
        .setPartitions(md._partitions)
        .build();
  }

  private FieldSpec createKeyFieldSpec() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the abstraction around the Map index construction which provides the
   * API needed to perform Map value operations.
   *
   * @return MapIndexReader or <i>null</i> if this is not a map data source.
   */
  @Nullable
  private MapIndexReader getMapIndex() {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  private static class MutableMapDataSourceMetadata implements DataSourceMetadata {
    final FieldSpec _fieldSpec;
    final int _numDocs;
    final int _numValues;
    final int _maxNumValuesPerMVEntry;
    final int _cardinality;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final Comparable _minValue;
    final Comparable _maxValue;
    final int _maxRowLengthInBytes;
    final Set<String> _denseKeys;

    MutableMapDataSourceMetadata(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry,
        int cardinality, @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions,
        @Nullable Comparable minValue, @Nullable Comparable maxValue, int maxRowLengthInBytes) {
      _fieldSpec = fieldSpec;
      _numDocs = numDocs;
      _numValues = numValues;
      _maxNumValuesPerMVEntry = maxNumValuesPerMVEntry;
      if (partitionFunction != null) {
        _partitionFunction = partitionFunction;
        _partitions = partitions;
      } else {
        _partitionFunction = null;
        _partitions = null;
      }
      _minValue = minValue;
      _maxValue = maxValue;
      _cardinality = cardinality;
      _maxRowLengthInBytes = maxRowLengthInBytes;
      _denseKeys = new HashSet<>();
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      // NOTE: Mutable data source is never sorted
      return false;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int getNumValues() {
      return _numValues;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return _maxNumValuesPerMVEntry;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return _minValue;
    }

    @Override
    public Comparable getMaxValue() {
      return _maxValue;
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return _partitionFunction;
    }

    @Nullable
    @Override
    public Set<Integer> getPartitions() {
      return _partitions;
    }

    @Override
    public int getCardinality() {
      return _cardinality;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return _maxRowLengthInBytes;
    }
  }
}
