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
package org.apache.pinot.segment.local.segment.index.forward.mutable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.map.MapDenseColumn;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MutableMapDenseColumnTest {
  private PinotDataBufferMemoryManager _memoryManager;
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MutableMapDenseColumnTest");
  private static final File INDEX_FILE =
      new File(TEMP_DIR, "testColumn" + V1Constants.Indexes.MUTABLE_MAP_FORWARD_INDEX_FILE_EXTENSION);

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(MutableMapDenseColumnTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public  void testAddOneKeyIntegerValue()
      throws IOException {
    MapDenseColumn mdc = new MapDenseColumn(100, _memoryManager, 1000, false, false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", 1);
    mdc.add(data, 0);

    data = new HashMap<>();
    data.put("a", 2);
    mdc.add(data, 1);

    ForwardIndexReader reader = mdc.getKeyReader("a");
    ForwardIndexReaderContext ctx = reader.createContext();
    int result = reader.getInt(0, ctx);
    Assert.assertEquals(result, 1);

    result = reader.getInt(1, ctx);
    Assert.assertEquals(result, 2);
  }

  @Test
  public  void testAddOneKeyStringValue()
      throws IOException {
    MapDenseColumn mdc = new MapDenseColumn(100, _memoryManager, 1000, false, false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", "hello");
    mdc.add(data, 0);

    data = new HashMap<>();
    data.put("a", "world");
    mdc.add(data, 1);

    ForwardIndexReader reader = mdc.getKeyReader("a");
    ForwardIndexReaderContext ctx = reader.createContext();
    String result = reader.getString(0, ctx);
    Assert.assertEquals(result, "hello");

    result = reader.getString(1, ctx);
    Assert.assertEquals(result, "world");
  }

  @Test
  public  void testAddTwoKeyIntegerValue()
      throws IOException {
    MapDenseColumn mdc = new MapDenseColumn(100, _memoryManager, 1000, false, false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", 1);
    mdc.add(data, 0);

    data = new HashMap<>();
    data.put("b", 2);
    mdc.add(data, 1);

    ForwardIndexReader aReader = mdc.getKeyReader("a");
    ForwardIndexReaderContext ctx = aReader.createContext();
    int result = aReader.getInt(0, ctx);
    Assert.assertEquals(result, 1);

    ForwardIndexReader bReader = mdc.getKeyReader("b");
    result = bReader.getInt(1, ctx);
    Assert.assertEquals(result, 2);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public  void testExceedMaxKeys()
      throws IOException {
    MapDenseColumn mdc = new MapDenseColumn(5, _memoryManager, 1000, false, false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", 1);
    data.put("b", 1);
    data.put("c", 1);
    data.put("d", 1);
    data.put("e", 1);
    mdc.add(data, 0);

    data = new HashMap<>();
    data.put("f", 2);
    mdc.add(data, 1);
  }
}
