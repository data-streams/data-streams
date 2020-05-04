package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

public interface InvalidatingBucket {

  /**
   * Inserts data item in list according to its meta
   */
  void insert(DataItem dataItem);

  /**
   * @param index within bucket
   * @return DataItem that located on the index position
   * @throws IndexOutOfBoundsException if the index is out of range
   */
  DataItem get(int index);

  /**
   * @param fromIndex low endpoint (inclusive) of the range
   * @param toIndex   high endpoint (exclusive) of the range
   * @param consumer for emitting items
   * @throws IndexOutOfBoundsException if at least one index is out of range
   */
  void forRange(int fromIndex, int toIndex, Consumer<DataItem> consumer);

  /**
   * Removes the elements of bucket between fromIndex and toIndex
   *
   * @param fromIndex low endpoint (inclusive) of the range
   * @param toIndex   high endpoint (exclusive) of the range
   */
  void clearRange(int fromIndex, int toIndex);

  /**
   * @return the size of this bucket
   */
  int size();

  /**
   * @return true if bucket is empty, otherwise return false
   */
  boolean isEmpty();

  /**
   * Returns an index of the first element in the bucket that is not less than (i.e. greater or equal to) value,
   * or last if no such element is found.
   * <p>
   * min i : a[i] >= x
   */
  int lowerBound(GlobalTime meta);

  /**
   * Returns an index of the first element in the bucket that is not less than (i.e. greater or equal to) value,
   * or last if no such element is found.
   * <p>
   * min i : a[i] >= x
   */
  int insertionPosition(DataItem meta);

  /**
   *
   * @param globalTime of last item in sub bucket
   * @param window for offset
   * @return bucket that contains [lowerBound(globalTime) - window + 1, lowerBound(globalTime)) items
   */
  InvalidatingBucket subBucket(GlobalTime globalTime, int window);
}
