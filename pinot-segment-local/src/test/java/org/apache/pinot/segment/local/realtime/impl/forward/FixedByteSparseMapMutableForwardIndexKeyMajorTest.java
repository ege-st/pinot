package org.apache.pinot.segment.local.realtime.impl.forward;

import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class FixedByteSparseMapMutableForwardIndexKeyMajorTest {
  private static final int NROWS = 1000;
  private static final int MAX_N_VALUES = 1000;
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setup() {
    _memoryManager = new DirectMemoryManager(FixedByteSparseMapMutableForwardIndexKeyMajorTest.class.getName());
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
    var index = new FixedByteKeyMajorMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    index.setIntMap(0, "k1", 15);
    var actualValue = index.getIntMapValue(0, "k1");
    assertEquals(actualValue, 15);
  }

  @Test
  public void testManyValuesOneKey() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteKeyMajorMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    for(int id = 0; id < 100; id++ ){
      index.setIntMap(id, "k1", id * 2);
    }

    for(int id = 0; id < 100; id++) {
      var actualValue = index.getIntMapValue(id, "k1");
      assertEquals(actualValue, id * 2);
    }
  }

  @Test
  public void testGetKeyNotPresent() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteKeyMajorMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    index.setIntMap(0, "k1", 15);
    // Get a key that this doc does not have
    var actualValue = index.getIntMapValue(0, "k5");

    // Start with 0 but this should be the Null symbol
    assertEquals(actualValue, 0);
  }

  @Test
  public void testManyKeys() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteKeyMajorMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    for(int id = 0; id < 100; id++ ){
      index.setIntMap(id, String.format("%d", id), id * 2);
    }

    for(int id = 0; id < 100; id++) {
      var actualValue = index.getIntMapValue(id, String.format("%d", id));
      assertEquals(actualValue, id * 2);

      // Check that this doc does not have a value for a key that it does not have in its map
      var actualValue2 = index.getIntMapValue(id, String.format("%d", id+1));
      assertEquals(actualValue2, 0);
    }
  }

  @Test
  public void testDocHasKeysButGetKeyItDoesNotHave() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteKeyMajorMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    index.setIntMap(0, "k1", 10);
    index.setIntMap(0, "k2", 11);
    index.setIntMap(0, "k3", 12);
    assertEquals(index.getIntMapValue(0, "k1"), 10);
    assertEquals(index.getIntMapValue(0, "k2"), 11);
    assertEquals(index.getIntMapValue(0, "k3"), 12);

    // Have a different doc have a value for key "k3"
    index.setIntMap(1, "k1", 20);
    index.setIntMap(1, "k2", 21);
    assertEquals(index.getIntMapValue(1, "k1"), 20);
    assertEquals(index.getIntMapValue(1, "k2"), 21);

    index.setIntMap(2, "k1", 30);
    index.setIntMap(2, "k2", 31);
    index.setIntMap(2, "k3", 32);
    assertEquals(index.getIntMapValue(2, "k1"), 30);
    assertEquals(index.getIntMapValue(2, "k2"), 31);
    assertEquals(index.getIntMapValue(2, "k3"), 32);

    index.setIntMap(4, "k1", 30);
    index.setIntMap(4, "k2", 31);
    index.setIntMap(4, "k3", 32);
    assertEquals(index.getIntMapValue(4, "k1"), 30);
    assertEquals(index.getIntMapValue(4, "k2"), 31);
    assertEquals(index.getIntMapValue(4, "k3"), 32);

    // Try to get the value of "k3" for docId 0.  This should return null or 0.
    var actualValue = index.getIntMapValue(1, "k3");
    assertEquals(actualValue, 0);

    // Check that docid 3 has no keys
    assertEquals(index.getIntMapValue(3, "k1"), 0);
    assertEquals(index.getIntMapValue(3, "k2"), 0);
    assertEquals(index.getIntMapValue(3, "k3"), 0);
  }

  @Test
  public void testHitBufferSizeLimit() {
    // Buffer size limit is NROWS per key
    // so add docs to one key until NROWS is exceeded

    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteKeyMajorMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    // Fill up the buffer
    for(int id = 0; id < NROWS; id++ ) {
      index.setIntMap(id, "k5", id * 2);
    }

    // Exceed the key buffer size
    for(int id = NROWS; id <= NROWS + 5; id++ ) {
      index.setIntMap(id, "k5", id * 2);
    }
  }
}
