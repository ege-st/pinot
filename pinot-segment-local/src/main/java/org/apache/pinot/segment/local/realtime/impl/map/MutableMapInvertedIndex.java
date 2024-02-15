package org.apache.pinot.segment.local.realtime.impl.map;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.config.table.MapInvertedIndexConfig;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class MutableMapInvertedIndex implements InvertedIndexReader<MutableRoaringBitmap>, MutableIndex {
  private final MapInvertedIndexConfig _config;
  private final HashMap<String, ThreadSafeMutableRoaringBitmap> _kvBitmaps = new HashMap<>();
  private final HashMap<String, ThreadSafeMutableRoaringBitmap> _keyBitmaps = new HashMap<>();
  private final HashMap<String, ThreadSafeMutableRoaringBitmap> _valueBitmaps = new HashMap<>();
  private final ReentrantReadWriteLock.ReadLock _readLock;
  private final ReentrantReadWriteLock.WriteLock _writeLock;

  public MutableMapInvertedIndex(MapInvertedIndexConfig config) {
    _config = config;
    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    _readLock = readWriteLock.readLock();
    _writeLock = readWriteLock.writeLock();
  }

  public MutableRoaringBitmap getDocIdsWithKey(String key) {
    ThreadSafeMutableRoaringBitmap bitmap;
    try {
      _readLock.lock();
      // NOTE: the given dictionary id might not be added to the inverted index yet. We first add the value to the
      // dictionary. Before the value is added to the inverted index, the query might have predicates that match the
      // newly added value. In that case, the given dictionary id does not exist in the inverted index, and we return an
      // empty bitmap. For multi-valued column, the dictionary id might be larger than the bitmap size (not equal).
      if (!_keyBitmaps.containsKey(key)) {
        return new MutableRoaringBitmap();
      }
      bitmap = _keyBitmaps.get(key);
    } finally {
      _readLock.unlock();
    }
    return bitmap.getMutableRoaringBitmap();
  }

  public MutableRoaringBitmap getDocIdsWithValue(String value) {
    ThreadSafeMutableRoaringBitmap bitmap;
    try {
      _readLock.lock();
      bitmap = _valueBitmaps.get(value);
      if (bitmap == null) {
        return new MutableRoaringBitmap();
      }
    } finally {
      _readLock.unlock();
    }
    return bitmap.getMutableRoaringBitmap();
  }

  public MutableRoaringBitmap getDocIdsWithKeyValue(String key, String value) {
    ThreadSafeMutableRoaringBitmap bitmap;
    try {
      _readLock.lock();
      String kv = key + value;
      bitmap = _kvBitmaps.get(kv);
      if (bitmap == null) {
        return new MutableRoaringBitmap();
      }
    } finally {
      _readLock.unlock();
    }
    return bitmap.getMutableRoaringBitmap();
  }

  @Override
  public void close() {
  }

  @Override
  public void add(@Nonnull Object map, int dictId, int docId) {
    HashMap<String, Object> kvs = (HashMap<String, Object>) map;
    boolean fail = false;

    try {
      _writeLock.lock();
      for (Map.Entry<String, Object> kv : kvs.entrySet()) {
        // Iterate over the map
        // Add the key, the value, and the Key+Value to their respective roaring bitmaps
        String key = kv.getKey();
        _keyBitmaps.computeIfAbsent(key, _k -> new ThreadSafeMutableRoaringBitmap()).add(docId);

        String value = kv.getValue().toString();
        _valueBitmaps.computeIfAbsent(value, _v -> new ThreadSafeMutableRoaringBitmap()).add(docId);

        String tuple = key + value;
        _kvBitmaps.computeIfAbsent(tuple, _kv -> new ThreadSafeMutableRoaringBitmap()).add(docId);

        if (_keyBitmaps.size() > _config.getMaxEntries()
            || _valueBitmaps.size() > _config.getMaxEntries()
            || _kvBitmaps.size() > _config.getMaxEntries()) {
          throw new RuntimeException("Map Inverted Index has exceeded the maximum number of entries");
        }
      }
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MutableRoaringBitmap getDocIds(int dictId) {
    return null;
  }
}
