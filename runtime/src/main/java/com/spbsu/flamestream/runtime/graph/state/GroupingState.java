package com.spbsu.flamestream.runtime.graph.state;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.SynchronizedInvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.Grouping;

import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GroupingState {
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
  private final ConcurrentMap<Key, InvalidatingBucket> buffers;
  private TreeMap<Long, Set<Key>> timeKeys = new TreeMap<>();
  private long minTime = Long.MIN_VALUE;

  public GroupingState(Grouping<?> grouping) {
    this.grouping = grouping;
    buffers = new ConcurrentHashMap<>();
  }

  private GroupingState(Grouping<?> grouping, ConcurrentMap<Key, InvalidatingBucket> buffers) {
    this.grouping = grouping;
    this.buffers = buffers;
  }

  public InvalidatingBucket bucketFor(DataItem item) {
    final Key key = new Key(grouping, item);
    final OptionalLong keyMinTime =
            grouping.equalz().labels().stream().mapToLong(label -> item.labels().get(label).time).min();
    if (keyMinTime.isPresent()) {
      if (keyMinTime.getAsLong() < minTime) {
        throw new IllegalArgumentException();
      }
      timeKeys.computeIfAbsent(keyMinTime.getAsLong(), __ -> new HashSet<>()).add(key);
    }
    return buffers.computeIfAbsent(key, __ -> new SynchronizedInvalidatingBucket(grouping.order()));
  }

  public void onMinTime(long minTime) {
    if (minTime <= this.minTime) {
      throw new IllegalArgumentException();
    }
    this.minTime = minTime;
    while (!timeKeys.isEmpty()) {
      final Map.Entry<Long, Set<Key>> minTimeKeys = timeKeys.firstEntry();
      if (minTime <= minTimeKeys.getKey()) {
        break;
      }
      minTimeKeys.getValue().forEach(buffers::remove);
      timeKeys.pollFirstEntry();
    }
    System.out.println("buffers.size() == " + buffers.size());
  }

  public GroupingState subState(GlobalTime ceil, int window) {
    final ConcurrentMap<Key, InvalidatingBucket> subState = new ConcurrentHashMap<>();
    buffers.forEach((key, bucket) -> subState.put(key, bucket.subBucket(ceil, window)));
    return new GroupingState(grouping, subState);
  }
}
