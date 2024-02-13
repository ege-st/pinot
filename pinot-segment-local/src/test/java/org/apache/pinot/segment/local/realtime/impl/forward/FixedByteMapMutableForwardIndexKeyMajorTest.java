package org.apache.pinot.segment.local.realtime.impl.forward;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.map.MutableMapForwardIndex;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class FixedByteMapMutableForwardIndexKeyMajorTest {
  private static final int NROWS = 1000;
  private static final int MAX_N_VALUES = 1000;
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setup() {
    _memoryManager = new DirectMemoryManager(FixedByteMapMutableForwardIndexKeyMajorTest.class.getName());
  }

  @AfterClass
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testSetKeyValue() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new MutableMapForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext,
        Set.of("k1")
    );

    index.setIntMapKeyValue(0, "k1", 15);
    var actualValue = index.getIntMapKeyValue(0, "k1");
    assertEquals(actualValue, 15);
  }

  @Test
  public void testManyValuesOneKey() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new MutableMapForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext,
        Set.of("k1")
    );

    for(int id = 0; id < 100; id++ ){
      index.setIntMapKeyValue(id, "k1", id * 2);
    }

    for(int id = 0; id < 100; id++) {
      var actualValue = index.getIntMapKeyValue(id, "k1");
      assertEquals(actualValue, id * 2);
    }
  }

  @Test
  public void testGetKeyNotPresent() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new MutableMapForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext,
        Set.of("k1")
    );

    index.setIntMapKeyValue(0, "k1", 15);
    // Get a key that this doc does not have
    var actualValue = index.getIntMapKeyValue(0, "k5");

    // Start with 0 but this should be the Null symbol
    assertEquals(actualValue, 0);
  }

  @Test
  public void testManyKeys() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    Set<String> denseKeys = IntStream.range(0, 100).mapToObj(id -> String.format("%d", id)).collect(Collectors.toSet());
    var index = new MutableMapForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext,
        denseKeys
    );

    for(int id = 0; id < 100; id++ ){
      index.setIntMapKeyValue(id, String.format("%d", id), id * 2);
    }

    for(int id = 0; id < 100; id++) {
      var actualValue = index.getIntMapKeyValue(id, String.format("%d", id));
      assertEquals(actualValue, id * 2);

      // Check that this doc does not have a value for a key that it does not have in its map
      var actualValue2 = index.getIntMapKeyValue(id, String.format("%d", id+1));
      assertEquals(actualValue2, 0);
    }
  }

  @Test
  public void testDocHasKeysButGetKeyItDoesNotHave() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new MutableMapForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext,
        Set.of("k1", "k2", "k3")
    );

    index.setIntMapKeyValue(0, "k1", 10);
    index.setIntMapKeyValue(0, "k2", 11);
    index.setIntMapKeyValue(0, "k3", 12);
    assertEquals(index.getIntMapKeyValue(0, "k1"), 10);
    assertEquals(index.getIntMapKeyValue(0, "k2"), 11);
    assertEquals(index.getIntMapKeyValue(0, "k3"), 12);

    // Have a different doc have a value for key "k3"
    index.setIntMapKeyValue(1, "k1", 20);
    index.setIntMapKeyValue(1, "k2", 21);
    assertEquals(index.getIntMapKeyValue(1, "k1"), 20);
    assertEquals(index.getIntMapKeyValue(1, "k2"), 21);

    index.setIntMapKeyValue(2, "k1", 30);
    index.setIntMapKeyValue(2, "k2", 31);
    index.setIntMapKeyValue(2, "k3", 32);
    assertEquals(index.getIntMapKeyValue(2, "k1"), 30);
    assertEquals(index.getIntMapKeyValue(2, "k2"), 31);
    assertEquals(index.getIntMapKeyValue(2, "k3"), 32);

    index.setIntMapKeyValue(4, "k1", 30);
    index.setIntMapKeyValue(4, "k2", 31);
    index.setIntMapKeyValue(4, "k3", 32);
    assertEquals(index.getIntMapKeyValue(4, "k1"), 30);
    assertEquals(index.getIntMapKeyValue(4, "k2"), 31);
    assertEquals(index.getIntMapKeyValue(4, "k3"), 32);

    // Try to get the value of "k3" for docId 0.  This should return null or 0.
    var actualValue = index.getIntMapKeyValue(1, "k3");
    assertEquals(actualValue, 0);

    // Check that docid 3 has no keys
    assertEquals(index.getIntMapKeyValue(3, "k1"), 0);
    assertEquals(index.getIntMapKeyValue(3, "k2"), 0);
    assertEquals(index.getIntMapKeyValue(3, "k3"), 0);
  }

  @Test
  public void testHitBufferSizeLimit() {
    // Buffer size limit is NROWS per key
    // so add docs to one key until NROWS is exceeded

    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new MutableMapForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext,
        Set.of("k5")
    );

    // Fill up the buffer
    for(int id = 0; id < NROWS; id++ ) {
      index.setIntMapKeyValue(id, "k5", id * 2);
    }

    // Exceed the key buffer size
    for(int id = NROWS; id <= NROWS + 5; id++ ) {
      index.setIntMapKeyValue(id, "k5", id * 2);
    }
  }
}
