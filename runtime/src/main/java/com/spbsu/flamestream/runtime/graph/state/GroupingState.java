package com.spbsu.flamestream.runtime.graph.state;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.SynchronizedInvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.Grouping;

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

  public GroupingState(Grouping<?> grouping) {
    this.grouping = grouping;
    buffers = new ConcurrentHashMap<>();
  }

  private GroupingState(Grouping<?> grouping, ConcurrentMap<Key, InvalidatingBucket> buffers) {
    this.grouping = grouping;
    this.buffers = buffers;
  }

  public InvalidatingBucket bucketFor(DataItem item) {
    return buffers.computeIfAbsent(
            new Key(grouping, item),
            __ -> new SynchronizedInvalidatingBucket(grouping.order())
    );
  }

  public GroupingState subState(GlobalTime ceil, int window) {
    final ConcurrentMap<Key, InvalidatingBucket> subState = new ConcurrentHashMap<>();
    buffers.forEach((key, bucket) -> subState.put(key, bucket.subBucket(ceil, window)));
    return new GroupingState(grouping, subState);
  }
}
