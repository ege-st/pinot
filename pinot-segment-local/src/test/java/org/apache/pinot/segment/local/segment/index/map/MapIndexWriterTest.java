package org.apache.pinot.segment.local.segment.index.map;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.map.MapIndexCreator;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MapIndexWriterTest {
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
  public void testingWritingMultipleChunks() {
    List<String> keys = List.of("a", "b");
    List<FieldSpec.DataType> keyTypes = List.of(FieldSpec.DataType.INT, FieldSpec.DataType.INT);
    List<HashMap<String, Object>> records = createTestData(keys, keyTypes, 2000);
    MapIndexConfig config = new MapIndexConfig();
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

  @Test
  public void testingWithStringType() {
    // Create test data
    List<String> keys = List.of("a");
    List<FieldSpec.DataType> valueTypes = List.of(
        FieldSpec.DataType.STRING
    );
    List<HashMap<String, Object>> records = createTestData(keys, valueTypes, 2000);

    // Configure map index
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setDenseKeyTypes(valueTypes);
    config.setMaxKeys(4);
    try {
      createIndex(config, records);
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  @Test
  public void testingWithDifferentTypes() {
    // Create test data
    List<String> keys = List.of("a", "b", "c", "d", "e", "f");
    List<FieldSpec.DataType> valueTypes = List.of(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.LONG,
        FieldSpec.DataType.FLOAT,
        FieldSpec.DataType.DOUBLE,
        FieldSpec.DataType.BOOLEAN,
        FieldSpec.DataType.STRING
    );
    List<HashMap<String, Object>> records = createTestData(keys, valueTypes, 2000);

    // Configure map index
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setDenseKeyTypes(valueTypes);
    config.setMaxKeys(4);
    try {
      createIndex(config, records);
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  @Test
  public void typeMismatch() {
    // If a key from the input Map Value has a type that differs from the already defined type of the dense key then
    // Write the default value to the column.
    List<String> keys = List.of("a", "b");
    List<FieldSpec.DataType> valueTypes = List.of(FieldSpec.DataType.STRING, FieldSpec.DataType.INT);
    List<HashMap<String, Object>> records = createTestData(keys,  valueTypes, 2000);
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setDenseKeyTypes(List.of(FieldSpec.DataType.STRING, FieldSpec.DataType.STRING));
    config.setMaxKeys(4);
    try {
      createIndex(config, records);
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  private List<HashMap<String, Object>> createTestData(List<String> keys, List<FieldSpec.DataType> keyTypes,
      int numRecords) {
    HashMap<String, Object> record = new HashMap<>();

    for(int i = 0; i < keys.size(); i++) {
      record.put(keys.get(i), generateTestValue(keyTypes.get(i)));
    }

    ArrayList<HashMap<String, Object>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      records.add(record);
    }

    return records;
  }

  private Object generateTestValue(FieldSpec.DataType type) {
    switch(type) {

      case INT:
        return 1;
      case LONG:
        return 2L;
      case FLOAT:
        return 3.0F;
      case DOUBLE:
        return 4.5D;
      case BOOLEAN:
        return true;
      case TIMESTAMP:
      case STRING:
        return "hello";
      case JSON:
      case BIG_DECIMAL:
      case BYTES:
      case STRUCT:
      case MAP:
      case LIST:
      case UNKNOWN:
        throw new UnsupportedOperationException();
    }

    throw new UnsupportedOperationException();
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
        .withLengthOfLongestEntry(30)
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
