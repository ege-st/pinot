package org.apache.pinot.segment.local.segment.index.map;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.map.MapIndexCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MapIndexTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonIndexTest");
  private static final String MAP_COLUMN_NAME = "dense_map";

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
  public void simpleTest() {
    List<String> keys = List.of("a", "b");
    List<HashMap<String, Object>> records = createTestData(keys, 2000);
    File denseColumnIndexFile = new File(INDEX_DIR, MAP_COLUMN_NAME + V1Constants.Indexes.MAP_DENSE_INDEX_FILE_EXTENSION);
    MapIndexConfig config = new MapIndexConfig();
    //config.setDenseKeys(List.of("a", "b", "c", "d"));
    config.setDenseKeys(keys);
    config.setDenseKeyTypes(List.of(FieldSpec.DataType.INT, FieldSpec.DataType.INT, FieldSpec.DataType.INT,
        FieldSpec.DataType.INT));
    config.setMaxKeys(4);
    try {
      createIndex(config, records);
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  private List<HashMap<String, Object>> createTestData(List<String> keys, int numRecords) {
    HashMap<String, Object> record = new HashMap<>();

    for(String key: keys) {
      record.put(key, 1);
    }

    ArrayList<HashMap<String, Object>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      records.add(record);
    }

    return records;
  }

  /**
   * Creates a Dense Map index with the given config and adds the given records
   * @param records
   * @throws IOException on error
   */
  private void createIndex(MapIndexConfig mapIndexConfig, List<HashMap<String, Object>> records)
      throws IOException {
    DimensionFieldSpec mapSpec = new DimensionFieldSpec();
    mapSpec.setDataType(FieldSpec.DataType.MAP);
    mapSpec.setName(MAP_COLUMN_NAME);
    IndexCreationContext context = new IndexCreationContext.Common.Builder()
        .withIndexDir(INDEX_DIR)
        .withTotalDocs(records.size())
        .sorted(false)
        .onHeap(false)
        .withDictionary(false)
        .withFieldSpec(mapSpec)
        .build();
    try (org.apache.pinot.segment.spi.index.creator.MapIndexCreator indexCreator =  new MapIndexCreator(context, MAP_COLUMN_NAME, mapIndexConfig)) {
      for (int i = 0; i < records.size(); i++) {
        indexCreator.add(records.get(i));
      }
      indexCreator.seal();
    }
  }
}
