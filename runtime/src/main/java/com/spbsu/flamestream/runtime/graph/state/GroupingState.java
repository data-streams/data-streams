package com.spbsu.flamestream.runtime.graph.state;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.SynchronizedInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.TimedMap;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.Grouping;

public class GroupingState {
  public static class Key {
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
  private final TimedMap<Key, SynchronizedInvalidatingBucket> map;

  public GroupingState(Grouping<?> grouping) {
    this.grouping = grouping;
    map = new TimedMap<>(
            () -> new SynchronizedInvalidatingBucket(grouping.order()),
            grouping.equalz().labels().isEmpty() ? null : key -> grouping.equalz().labels().stream().mapToLong(label ->
                    key.dataItem.meta().labels().get(label).globalTime.time()
            ).min().getAsLong()
    );
  }

  private GroupingState(Grouping<?> grouping, TimedMap<Key, SynchronizedInvalidatingBucket> map) {
    this.grouping = grouping;
    this.map = map;
  }

  public InvalidatingBucket bucketFor(DataItem item) {
    return map.get(new Key(grouping, item));
  }

  public void onMinTime(long minTime) {
    map.onMinTime(minTime);
  }

  public GroupingState subState(GlobalTime ceil) {
    return new GroupingState(grouping, map.map(bucket -> bucket.subBucket(ceil, grouping.window())));
  }
}
