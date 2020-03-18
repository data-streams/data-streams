package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

public class Grouping<T> extends HashingVertexStub {
  private static ConcurrentHashMap<Object, LongAdder> classTombstonesNumber = new ConcurrentHashMap<>();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("classTombstonesNumber " + classTombstonesNumber);
    }));
  }

  private final HashFunction hash;
  private final Equalz equalz;
  private final int window;
  private final Class<?> clazz;
  private final boolean undoPartialWindows;
  private final SerializableComparator<DataItem> order;

  public static class Builder {
    private final HashFunction hash;
    private final Equalz equalz;
    private final int window;
    private final Class<?> clazz;
    private boolean undoPartialWindows = false;
    private SerializableComparator<DataItem> order = (dataItem1, dataItem2) -> 0;

    public Builder(HashFunction hash, Equalz equalz, int window, Class<?> clazz) {
      this.window = window;
      this.hash = hash;
      this.equalz = equalz;
      this.clazz = clazz;
    }

    public Builder undoPartialWindows(boolean undoPartialWindows) {
      this.undoPartialWindows = undoPartialWindows;
      return this;
    }

    public Builder order(SerializableComparator<DataItem> order) {
      this.order = order;
      return this;
    }
  }

  public Grouping(HashFunction hash, Equalz equalz, int window, Class<?> clazz) {
    this.window = window;
    this.hash = hash;
    this.equalz = equalz;
    this.clazz = clazz;
    undoPartialWindows = false;
    this.order = (dataItem1, dataItem2) -> 0;
  }

  public Grouping(Builder builder) {
    this.window = builder.window;
    this.hash = builder.hash;
    this.equalz = builder.equalz;
    this.clazz = builder.clazz;
    this.undoPartialWindows = builder.undoPartialWindows;
    this.order = builder.order;
  }

  public HashFunction hash() {
    return hash;
  }

  public Equalz equalz() {
    return equalz;
  }

  public int window() {
    return window;
  }

  public GroupingOperation operation(long physicalId) {
    return new GroupingOperation(physicalId);
  }

  @Override
  public String toString() {
    return "Grouping{" +
            "hash=" + hash +
            ", equalz=" + equalz +
            ", window=" + window +
            '}';
  }

  public SerializableComparator<DataItem> order() {
    return order;
  }

  public class GroupingOperation {
    private final long physicalId;

    GroupingOperation(long physicalId) {
      this.physicalId = physicalId;
    }

    public Stream<DataItem> apply(DataItem dataItem, InvalidatingBucket bucket) {
      final Collection<DataItem> items = new ArrayList<>();

      if (!dataItem.meta().isTombstone()) {
        final int positionToBeInserted = bucket.insertionPosition(dataItem);
        items.addAll(replayAround(positionToBeInserted, bucket, true, false));
        bucket.insert(dataItem);
        items.addAll(replayAround(positionToBeInserted, bucket, false, true));
      } else {
        final int positionToBeCleared = bucket.insertionPosition(dataItem) - 1;
        items.addAll(replayAround(positionToBeCleared, bucket, true, true));
        bucket.insert(dataItem);
        items.addAll(replayAround(positionToBeCleared, bucket, false, false));
      }

      return items.stream();
    }

    private List<DataItem> replayAround(
            int index,
            InvalidatingBucket bucket,
            boolean areTombs,
            boolean include
    ) {
      final int limit = (int) Math.min((long) index + window - (include ? 0 : 1), bucket.size());
      if (undoPartialWindows) {
        if (bucket.isEmpty()) {
          return Collections.emptyList();
        } else if (bucket.size() < window) {
          return Collections.singletonList(bucketWindow(bucket, 0, bucket.size(), areTombs));
        } else {
          final List<DataItem> items = new ArrayList<>();
          for (int right = Math.max(index + 1, window); right <= limit; ++right) {
            items.add(bucketWindow(bucket, Math.max(right - window, 0), right, areTombs));
          }
          return items;
        }
      } else {
        final List<DataItem> items = new ArrayList<>();
        for (int right = index + 1; right <= limit; ++right) {
          items.add(bucketWindow(bucket, Math.max(right - window, 0), right, areTombs));
        }
        return items;
      }
    }

    private PayloadDataItem bucketWindow(InvalidatingBucket bucket, int left, int right, boolean areTombs) {
      if (areTombs) {
        classTombstonesNumber.computeIfAbsent(this, __ -> new LongAdder()).increment();
      }
      final List<T> groupingResult = new ArrayList<>();
      //noinspection unchecked
      bucket.forRange(left, right, dataItem -> groupingResult.add(dataItem.payload((Class<T>) clazz)));
      return new PayloadDataItem(
              new Meta(bucket.get(right - 1).meta(), physicalId, areTombs),
              groupingResult,
              bucket.get(right - 1).labels()
      );
    }
  }
}
