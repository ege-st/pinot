package org.apache.pinot.segment.local.realtime.impl.forward;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManagerTest;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexUtil;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10)
@Measurement(iterations = 5, time = 10)
@State(Scope.Benchmark)

public class FixedByteSparseMapMutableForwardIndexBenchmark {
  private static final int NROWS = 1000;
  private static final int MAX_N_VALUES = 1000;
  private PinotDataBufferMemoryManager _memoryManager;
  private FixedByteKeyMajorMapMutableForwardIndex _index;
  private String _tmpDir;

  @BeforeClass
  public void setup() {
    _tmpDir = System.getProperty("java.io.tmpdir") + "/" + MmapMemoryManagerTest.class.getSimpleName();
    File dir = new File(_tmpDir);
    FileUtils.deleteQuietly(dir);
    dir.mkdir();
    dir.deleteOnExit();

    _memoryManager = new MmapMemoryManager(_tmpDir, FixedByteSparseMapMutableForwardIndexKeyMajorTest.class.getName());
    String allocationContext =
        IndexUtil.buildAllocationContext("testSegment", "testMapCol",
            V1Constants.Indexes.RAW_MAPSV_FORWARD_INDEX_FILE_EXTENSION);
    _index = new FixedByteKeyMajorMapMutableForwardIndex(
        FieldSpec.DataType.INT,
        FieldSpec.DataType.INT.size(),
        NROWS,
        _memoryManager,
        allocationContext
    );
  }

  @AfterClass
  public void tearDown() throws Exception {
    _memoryManager.close();
    new File(_tmpDir).delete();
  }

  @Test
  public void insertKV() {
    // TODO(ERICH): Very hacky benchmark until I figure out how to run JMH.
    {
      var startNs = System.nanoTime();
      for(int i = 0; i < 100_000_000; i++) {
        _index.setIntMapKeyValue(i, "k1", i);
      }
      var endNs = System.nanoTime();
      System.out.printf("Duration: %d\n", (endNs - startNs)/1000000);
    }
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
