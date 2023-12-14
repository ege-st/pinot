package org.apache.pinot.segment.local.realtime.impl.forward;

import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.dictionary.MultiValueDictionaryTest;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class FixedByteSparseMapMutableForwardIndexTest {
  private static final int NROWS = 1000;
  private static final int MAX_N_VALUES = 1000;
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setup() {
    _memoryManager = new DirectMemoryManager(FixedByteSparseMapMutableForwardIndexTest.class.getName());
  }

  @AfterClass
  public void tearDown() throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testSetKeyValue() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteSparseMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    index.setIntMap(0, 1, 15);
    var actualValue = index.getIntMap(0, 1);
    assertEquals(actualValue, 15);
  }

  @Test
  public void testManyValuesOneKey() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteSparseMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    for(int id = 0; id < 100; id++ ){
      index.setIntMap(id, 1, id * 2);
    }

    for(int id = 0; id < 100; id++) {
      var actualValue = index.getIntMap(id, 1);
      assertEquals(actualValue, id * 2);
    }
  }

  @Test
  public void testGetKeyNotPresent() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteSparseMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    index.setIntMap(0, 1, 15);
    // Get a key that this doc does not have
    var actualValue = index.getIntMap(0, 5);

    // Start with 0 but this should be the Null symbol
    assertEquals(actualValue, 0);
  }

  @Test
  public void testManyKeys() {
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteSparseMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    for(int id = 0; id < 100; id++ ){
      index.setIntMap(id, id, id * 2);
    }

    for(int id = 0; id < 100; id++) {
      var actualValue = index.getIntMap(id, id);
      assertEquals(actualValue, id * 2);

      // Check that this doc does not have a value for a key that it does not have in its map
      var actualValue2 = index.getIntMap(id, id+1);
      assertEquals(actualValue2, 0);
    }
  }

  @Test
  public void testHitBufferSizeLimit() {
    // Buffer size limit is NROWS per key
    // so add docs to one key until NROWS is exceeded

    final int NROWS = 10;
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);
    var index = new FixedByteSparseMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );

    // Fill up the buffer
    for(int id = 0; id < NROWS-1; id++ ){
      index.setIntMap(id, 5, id * 2);
    }

    // Exceed the key buffer size
    // TODO: currently this will fault but we'll want it to add a new buffer to the key's buffer set
    for(int id = NROWS; id <= NROWS + 5; id++ ){
      index.setIntMap(id, 5, id * 2);
    }
  }
}
