package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.SerializableComparator;

import java.util.function.Consumer;

public class SynchronizedInvalidatingBucket implements InvalidatingBucket {
  private final InvalidatingBucket bucket;

  public SynchronizedInvalidatingBucket(SerializableComparator<DataItem> order) {
    this(new ArrayInvalidatingBucket(order));
  }

  protected SynchronizedInvalidatingBucket(InvalidatingBucket bucket) {
    this.bucket = bucket;
  }

  @Override
  public synchronized void insert(DataItem insertee) {
    bucket.insert(insertee);
  }

  @Override
  public synchronized DataItem get(int index) {
    return bucket.get(index);
  }

  @Override
  public synchronized void forRange(int fromIndex, int toIndex, Consumer<DataItem> consumer) {
    bucket.forRange(fromIndex, toIndex, consumer);
  }

  @Override
  public synchronized void clearRange(int fromIndex, int toIndex) {
    bucket.clearRange(fromIndex, toIndex);
  }

  @Override
  public synchronized int size() {
    return bucket.size();
  }

  @Override
  public synchronized boolean isEmpty() {
    return bucket.isEmpty();
  }

  @Override
  public synchronized int insertionPosition(DataItem dataItem) {
    return bucket.insertionPosition(dataItem);
  }

  @Override
  public synchronized int lowerBound(GlobalTime globalTime) {
    return bucket.lowerBound(globalTime);
  }

  @Override
  public synchronized SynchronizedInvalidatingBucket subBucket(GlobalTime globalTime, int window) {
    return new SynchronizedInvalidatingBucket(bucket.subBucket(globalTime, window));
  }
}
