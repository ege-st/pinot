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
package org.apache.pinot.segment.local.segment.index;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndexImpl;
import org.apache.pinot.segment.local.realtime.impl.map.MutableMapInvertedIndex;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OnHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.json.JsonIndexType;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.MapInvertedIndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/**
 * Unit test for {@link JsonIndexCreator} and {@link JsonIndexReader}.
 */
public class MutableMapInvertedIndexTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "MapInvertedIndexTest");
  private static final String ON_HEAP_COLUMN_NAME = "onHeap";
  private static final String OFF_HEAP_COLUMN_NAME = "offHeap";

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testSmallIndex()
      throws Exception {
    // @formatter: off
    // CHECKSTYLE:OFF
    HashMap<String, String> data1 = new HashMap<>();
    data1.put("foo", "1");
    data1.put("bar", "2");
    data1.put("fizz", "1");
    data1.put("buzz", "3");

    HashMap<String, String> data2 = new HashMap<>();
    data2.put("foo", "1");
    data2.put("bar", "3");
    data2.put("fizz", "1");
    data2.put("buzz", "3");
    data2.put("unique", "4");

    //CHECKSTYLE:ON
    // @formatter: on
    MapInvertedIndexConfig mapInvertedIndexConfig = new MapInvertedIndexConfig();
    try (MutableMapInvertedIndex mii = new MutableMapInvertedIndex(mapInvertedIndexConfig) ) {
      mii.add(data1, 0, 1);
      mii.add(data2, 0, 2);
      Assert.assertEquals(mii.getDocIdsWithKey("foo").toArray(), new int[]{1, 2});
      Assert.assertEquals(mii.getDocIdsWithKey("bar").toArray(), new int[]{1, 2});
      Assert.assertEquals(mii.getDocIdsWithKey("fizz").toArray(), new int[]{1, 2});
      Assert.assertEquals(mii.getDocIdsWithKey("buzz").toArray(), new int[]{1, 2});
      Assert.assertEquals(mii.getDocIdsWithKey("unique").toArray(), new int[]{2});

      Assert.assertEquals(mii.getDocIdsWithKeyValue("foo", "1").toArray(), new int[]{1, 2});
      Assert.assertEquals(mii.getDocIdsWithKeyValue("bar", "2").toArray(), new int[]{1});
      Assert.assertEquals(mii.getDocIdsWithKeyValue("bar", "3").toArray(), new int[]{2});
      Assert.assertEquals(mii.getDocIdsWithKeyValue("fizz", "1").toArray(), new int[]{1, 2});
      Assert.assertEquals(mii.getDocIdsWithKeyValue("buzz", "3").toArray(), new int[]{1, 2});
      Assert.assertEquals(mii.getDocIdsWithKeyValue("unique", "4").toArray(), new int[]{2});

      Assert.assertEquals(mii.getDocIdsWithValue("1").toArray(), new int[]{1, 2});
      Assert.assertEquals(mii.getDocIdsWithValue("2").toArray(), new int[]{1});
      Assert.assertEquals(mii.getDocIdsWithValue("3").toArray(), new int[]{1, 2});
      Assert.assertEquals(mii.getDocIdsWithValue("4").toArray(), new int[]{2});
      Assert.assertEquals(mii.getDocIdsWithValue("5").toArray(), new int[]{});
    }
  }
}
