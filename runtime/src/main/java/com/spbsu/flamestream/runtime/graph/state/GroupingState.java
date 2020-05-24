package com.spbsu.flamestream.runtime.graph.state;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.SynchronizedInvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.Grouping;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class GroupingState {
  private static final long baseNanos = System.nanoTime();
  private static final DataOutputStream output;
  private static final Executor fileWriter = Executors.newSingleThreadExecutor();
  private static final AtomicInteger stateIdsSequence = new AtomicInteger(), keyIdsSequence = new AtomicInteger();

  static {
    try {
      output = new DataOutputStream(new BufferedOutputStream(
              new FileOutputStream("/tmp/grouping_events.bin"),
              1 << 22
      ));
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        fileWriter.execute(() -> {
          try {
            output.flush();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
      }));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static void logEvent(long time, boolean free, int keyId, int stateId) {
    fileWriter.execute(() -> {
      try {
        output.writeLong(time);
        output.writeBoolean(free);
        output.writeInt(keyId);
        output.writeInt(stateId);
      } catch (IOException exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  static class Buffer implements InvalidatingBucket {
    final InvalidatingBucket bucket;
    final int id;

    public Buffer(InvalidatingBucket bucket) {
      this(bucket, keyIdsSequence.getAndIncrement());
    }

    public Buffer(InvalidatingBucket bucket, int id) {
      this.bucket = bucket;
      this.id = id;
    }

    @Override
    public void insert(DataItem dataItem) {
      bucket.insert(dataItem);
    }

    @Override
    public DataItem get(int index) {
      return bucket.get(index);
    }

    @Override
    public void forRange(int fromIndex, int toIndex, Consumer<DataItem> consumer) {
      bucket.forRange(fromIndex, toIndex, consumer);
    }

    @Override
    public void clearRange(int fromIndex, int toIndex) {
      bucket.clearRange(fromIndex, toIndex);
    }

    @Override
    public int size() {
      return bucket.size();
    }

    @Override
    public boolean isEmpty() {
      return bucket.isEmpty();
    }

    @Override
    public int lowerBound(GlobalTime meta) {
      return bucket.lowerBound(meta);
    }

    @Override
    public int insertionPosition(DataItem meta) {
      return bucket.insertionPosition(meta);
    }

    @Override
    public synchronized Buffer subBucket(GlobalTime globalTime, int window) {
      return new Buffer(bucket.subBucket(globalTime, window), id);
    }
  }

  private static class Key {
    final Grouping<?> grouping;
    final DataItem dataItem;
    final int hashCode;

    Key(Grouping<?> grouping, DataItem dataItem) {
      this.grouping = grouping;
      this.dataItem = dataItem;
      hashCode = grouping.hash().applyAsInt(this.dataItem);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (obj instanceof Key) {
        final Key key = (Key) obj;
        return grouping.equalz().test(dataItem, key.dataItem);
      }
      return false;
    }
  }

  public final Grouping<?> grouping;
  private final ConcurrentMap<Key, Buffer> buffers;
  private TreeMap<Long, Set<Key>> timeKeys = new TreeMap<>();
  private long minTime = Long.MIN_VALUE;
  private final int id;

  public GroupingState(Grouping<?> grouping) {
    this.grouping = grouping;
    buffers = new ConcurrentHashMap<>();
    id = stateIdsSequence.getAndIncrement();
  }

  private GroupingState(Grouping<?> grouping, ConcurrentMap<Key, Buffer> buffers, int id) {
    this.grouping = grouping;
    this.buffers = buffers;
    this.id = id;
  }

  public InvalidatingBucket bucketFor(DataItem item) {
    final Key key = new Key(grouping, item);
    final OptionalLong keyMinTime =
            grouping.equalz().labels().stream().mapToLong(label -> item.meta().labels().get(label).globalTime.time()).min();
    if (keyMinTime.isPresent()) {
      if (keyMinTime.getAsLong() < minTime) {
        throw new IllegalArgumentException();
      }
      timeKeys.computeIfAbsent(keyMinTime.getAsLong(), __ -> new HashSet<>()).add(key);
    }
    final Buffer buffer = buffers.computeIfAbsent(
            key,
            __ -> new Buffer(new SynchronizedInvalidatingBucket(grouping.order()))
    );
    logEvent(System.nanoTime() - baseNanos, false, buffer.id, id);
    return buffer;
  }

  public void onMinTime(long minTime) {
    final long time = System.nanoTime() - baseNanos;
    if (minTime <= this.minTime) {
      throw new IllegalArgumentException();
    }
    this.minTime = minTime;
    while (!timeKeys.isEmpty()) {
      final Map.Entry<Long, Set<Key>> minTimeKeys = timeKeys.firstEntry();
      if (minTime <= minTimeKeys.getKey()) {
        break;
      }
      for (Key key : minTimeKeys.getValue()) {
        logEvent(time, true, buffers.remove(key).id, id);
      }
      timeKeys.pollFirstEntry();
    }
  }

  public GroupingState subState(GlobalTime ceil, int window) {
    final ConcurrentMap<Key, Buffer> subState = new ConcurrentHashMap<>();
    buffers.forEach((key, bucket) -> subState.put(key, bucket.subBucket(ceil, window)));
    return new GroupingState(grouping, subState, id);
  }
}
