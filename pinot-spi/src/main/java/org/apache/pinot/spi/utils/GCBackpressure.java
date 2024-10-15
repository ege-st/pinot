package org.apache.pinot.spi.utils;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides a mechanism for generating back pressure when Garbage Collector load becomes too high.
 * <p>
 * Use the factory method to get a BackPressure Instance. In your execution loop, place a call to the
 * `applyPressure` method.  If GC is high then this method will execute a Sleep operation for a number of
 * milliseconds forcing your execution loop to slow down and reduce the Heap pressure. If GC is not high
 * then the method will return immediately.
 * <p>
 *
 * Internally, this creates a single watcher thread which will monitor the GC behavior and turn on or off
 * backpressure. Threads can create "Clients" which will listen to the GC watcher and will apply backpressure
 * to the calling thread if GC is too high.
 */
public class GCBackpressure {
  private static final Logger LOGGER = LoggerFactory.getLogger(GCBackpressure.class);
  private static final int BUFFER_SIZE = 30;
  private static Timer _timer = null;
  private static final long[] _gcCountBuffer = new long[BUFFER_SIZE];
  private static final long[] _gcTimeBuffer = new long[BUFFER_SIZE]; // TODO: Is measuring gc time better than gc runs?
  private static volatile  int _multiplier = 0;
  private static final int MAX_MULTIPLIER = 4;
  private static int _count = 0;
  private static final long _periodMs = 2_000;
  private static GarbageCollectorMXBean _gcCollector = null;
  private static final long THRESHOLD = 50;
  private static final long MULTIPLY_THRESHOLD = 10;

  /**
   * If the GC Watcher is not initialized this will initialize it.  If it is initialized, then this will
   * do nothing.
   */
  private static void initializeOnce() throws Exception {
        /*
        Only one GC Watcher is needed to be running, because there is only one GC.
         */
    synchronized (GCBackpressure.class) {
      if(_timer == null && _gcCollector == null) {
        _gcCollector = getYoungGc();

        if (_gcCollector == null) {
          throw new Exception("G1 Young GC not found");
        }

        // Run as a daemon so that this does not require any coordination during shutdown.
        _timer = new Timer(true);

        _timer.scheduleAtFixedRate(new TimerTask() {
          @Override
          public void run() {
            updatePressureState();
          }
        }, 0, _periodMs);
      } else if (_timer == null || _gcCollector == null) {
        throw new IllegalStateException("Partially initialized");
      }
    }
  }

  static GarbageCollectorMXBean getYoungGc() throws Exception{
    // TODO: This should be more robust
    List<GarbageCollectorMXBean> gcs = ManagementFactory.getGarbageCollectorMXBeans();
    if (gcs.isEmpty()) {
      return null;
    }

    GarbageCollectorMXBean youngGc = null;
    for (GarbageCollectorMXBean gc : gcs) {
      if (gc.getName().equals("G1 Young Generation")) {
        youngGc = gc;
      }
    }

    return youngGc;
  }

  private static void updatePressureState() {
    long newest = recordGCMetrics();
    long oldest = getOldest();
    long diff = newest - oldest;

    LOGGER.info("GC Runs: {}. GC Time(ms): {}", diff, getGcTimeTotal());

    if (diff > THRESHOLD) {
      int prev = _multiplier;
      if (prev == 0) {
        _multiplier = 1;
        LOGGER.warn("Applying pressure");
      } else {
        // If the throttle is on, then check to see if the rate of GCs is within the threshold. If not, then
        // GCs are still being triggered too often, so double the back off multiplier.
        long prior = getPrior();

        if (newest - prior > MULTIPLY_THRESHOLD) {
          LOGGER.warn("Multiplying backoff pressure");
          _multiplier = Math.max(prev * 2, MAX_MULTIPLIER);
        }
      }
    } else {
      if (_multiplier != 0) {
        _multiplier = 0;
        LOGGER.warn("Stopped pressure");
      }
    }
  }

  private static long recordGCMetrics() {
    long gcCount = _gcCollector.getCollectionCount();
    _gcCountBuffer[_count % BUFFER_SIZE] = gcCount;

    long gcTime = _gcCollector.getCollectionTime();
    _gcTimeBuffer[_count % BUFFER_SIZE] = gcTime;


    _count++;

    return gcCount;
  }

  private static long getGcTimeTotal() {
    return _gcTimeBuffer[(_count - 1) % BUFFER_SIZE] - _gcTimeBuffer[_count % BUFFER_SIZE];
  }

  private static long getOldest() {
    return _gcCountBuffer[_count % BUFFER_SIZE];
  }

  private static long getPrior() {
    // _count points to the next spot in the buffer, so count-1 is the most recent entry and count-2 is the
    // second most recent entry
    return _gcCountBuffer[(_count-2) % BUFFER_SIZE];
  }

  public static GCBackPressureClient getClient(int baseMs, int maxMs) throws Exception {
    initializeOnce();
    return new GCBackPressureClient(baseMs, maxMs);
  }

  public static class GCBackPressureClient {
    final Random _random;
    final int _baseMs;
    final int _maxMs;

    GCBackPressureClient(int baseMs, int maxMs) {
      _random = new Random();
      _baseMs = baseMs;
      _maxMs = maxMs;
    }

    public void applyBackPressure() throws Exception {
      // Get the time that the GC watcher thinks should be use for back pressure
      int multiplier = GCBackpressure._multiplier;

      if (multiplier > 0) {
        int pause = Math.max(_baseMs * multiplier, _maxMs);
        // Add some fuzz to it to prevent many threads all pausing and then coming online at the same time
        pause -= _random.nextInt(pause/2);

        Thread.sleep(pause);
      }
    }
  }
}