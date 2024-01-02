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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.utils.ReadMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Tests for indexing map values into a segment.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MutableSegmentImplMapColumnTest {
  /* TODO Write tests for:
      - LONG
      - FLOAT
      - STRING
      - BOOLEAN
      - BYTES
      - JSON
      - DOUBLE
      - TIMESTAMP
      - Big Decimal
      - Test both key major and doc major models (for all types)
      - Test getting entire map (for all types)
   */
  private static final String AVRO_FILE = "data/test_data-mv.avro";
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MutableSegmentImplTest");

  private Schema _schema;
  private MutableSegmentImpl _mutableSegmentImpl;
  private ImmutableSegment _immutableSegment;
  private long _lastIndexedTs;
  private long _lastIngestionTimeMs;
  private long _startTimeMs;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);

    URL resourceUrl = MutableSegmentImplMapColumnTest.class.getClassLoader().getResource(AVRO_FILE);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());

    SegmentGeneratorConfig config =
        SegmentTestUtils.getRealtimeSegmentGeneratorConfigWithoutTimeColumnWithMapColumn(
            avroFile,
            TEMP_DIR,
            "testTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    _immutableSegment = ImmutableSegmentLoader.load(new File(TEMP_DIR, driver.getSegmentName()), ReadMode.mmap);

    _schema = config.getSchema();
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(_schema, "testSegment");

    // TODO(ERICH): needed to add myDim to the noDictionary set in order for it to _not_ be dictionary encoded.
    //    note that figuring out how to make it not dictionary encoded was a huge pain.
    _mutableSegmentImpl = MutableSegmentImplTestUtils
        .createMutableSegmentImpl(_schema, Set.of("myDim"), Collections.emptySet(), Collections.emptySet(),
            false);
    _lastIngestionTimeMs = System.currentTimeMillis();
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(_lastIngestionTimeMs, new GenericRow());
    _startTimeMs = System.currentTimeMillis();

    try (RecordReader recordReader = RecordReaderFactory
        .getRecordReader(FileFormat.AVRO, avroFile, _schema.getColumnNames(), null)) {
      GenericRow reuse = new GenericRow();
      //while (recordReader.hasNext()) {
      for(int i = 0; i < 10; i++ ){
        reuse.putValue("myCol", "Hello");
        reuse.putValue("myDim", Map.of("foo", i, "key", i*10));
        _mutableSegmentImpl.index(reuse, defaultMetadata);
        _lastIndexedTs = System.currentTimeMillis();
      }
    }
  }

  @Test
  public void testMetadata() {
    SegmentMetadata actualSegmentMetadata = _mutableSegmentImpl.getSegmentMetadata();
    SegmentMetadata expectedSegmentMetadata = _immutableSegment.getSegmentMetadata();
    assertEquals(actualSegmentMetadata.getTotalDocs(),10);

    // assert that the last indexed timestamp is close to what we expect
    long actualTs = _mutableSegmentImpl.getSegmentMetadata().getLastIndexedTimestamp();
    Assert.assertTrue(actualTs >= _startTimeMs);
    Assert.assertTrue(actualTs <= _lastIndexedTs);

    assertEquals(_mutableSegmentImpl.getSegmentMetadata().getLatestIngestionTimestamp(), _lastIngestionTimeMs);

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      DataSourceMetadata actualDataSourceMetadata = _mutableSegmentImpl.getDataSource(column).getDataSourceMetadata();
      DataSourceMetadata expectedDataSourceMetadata = _immutableSegment.getDataSource(column).getDataSourceMetadata();

      // TODO(ERICH): currently the datatype is just set to the Value type
      assertEquals(actualDataSourceMetadata.getDataType(), expectedDataSourceMetadata.getDataType());
      assertEquals(actualDataSourceMetadata.isSingleValue(), expectedDataSourceMetadata.isSingleValue());
      assertEquals(actualDataSourceMetadata.getNumDocs(), 10);

      // TODO(ERICH): expectedDatasourceMetadata needs to be configured to flag the map type columns
      /*if (!expectedDataSourceMetadata.isSingleValue() && !expectedDataSourceMetadata.isMapValue()) {
        assertEquals(actualDataSourceMetadata.getMaxNumValuesPerMVEntry(),
            expectedDataSourceMetadata.getMaxNumValuesPerMVEntry());
      }*/
    }
  }

  @Test
  public void testDataSourceForSVColumns()
      throws IOException {
    String column = "myDim";
    DataSource actualDataSource = _mutableSegmentImpl.getDataSource(column);
    int actualNumDocs = actualDataSource.getDataSourceMetadata().getNumDocs();
    int expectedNumDocs = 10; //expectedDataSource.getDataSourceMetadata().getNumDocs();
    assertEquals(actualNumDocs, expectedNumDocs);

    // Read through the contents of the map column
    Dictionary actualDictionary = actualDataSource.getDictionary();
    ForwardIndexReader actualReader = actualDataSource.getForwardIndex();
    try (ForwardIndexReaderContext actualReaderContext = actualReader.createContext()){
      for (int docId = 0; docId < expectedNumDocs; docId++) {
        var val = actualReader.getIntMapValue(docId, "foo");
        assertEquals(
            val,
            docId,
            column + "[foo] failed (IN the map)"
        );

        var val2 = actualReader.getIntMapValue(docId, "key");
        assertEquals(
            val2,
            docId * 10,
            column + "[key] failed (IN the map)"
        );

        var val3 = actualReader.getIntMapValue(docId, "bar");
        assertEquals(
            val3,
            0,
            column + "[bar] failed (NOT in the map)"
        );
      }
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
