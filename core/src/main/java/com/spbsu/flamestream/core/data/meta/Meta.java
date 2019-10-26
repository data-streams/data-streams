package com.spbsu.flamestream.core.data.meta;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public class Meta implements Comparable<Meta> {
  private static final int[] EMPTY_ARRAY = new int[0];

  public static final Comparator<Meta> NATURAL_ORDER = Comparator
          .comparing(Meta::globalTime)
          .thenComparing(Meta::childIds, new ArrayComparator())
          .thenComparing(Meta::trace)
          .thenComparing(Meta::isTombstone);


  private final GlobalTime globalTime;
  private final int[] childIds;
  private final long trace;
  private final boolean tombstone;

  public Meta(GlobalTime globalTime) {
    this.globalTime = globalTime;
    this.childIds = EMPTY_ARRAY;
    this.tombstone = false;
    this.trace = 0;
  }

  public Meta(Meta previous, long physicalId, boolean tombstone) {
    this.globalTime = previous.globalTime();
    this.childIds = previous.childIds;
    this.tombstone = tombstone;
    this.trace = previous.trace ^ physicalId;
  }

  public Meta(Meta previous, long physicalId, int childId, GlobalTime globalTime) {
    this.globalTime = globalTime;
    this.childIds = Arrays.copyOf(previous.childIds, previous.childIds.length + 1);
    childIds[childIds.length - 1] = childId;
    this.tombstone = previous.tombstone;
    this.trace = previous.trace ^ physicalId;
  }

  public boolean isTombstone() {
    return tombstone;
  }

  public GlobalTime globalTime() {
    return globalTime;
  }

  public long trace() {
    return trace;
  }

  int[] childIds() {
    return childIds;
  }

  @Override
  public int compareTo(Meta that) {
    return NATURAL_ORDER.compare(this, that);
  }

  public boolean isInvalidedBy(Meta that) {
    return !tombstone
            && that.tombstone
            && trace == that.trace
            && globalTime.equals(that.globalTime)
            && Arrays.equals(childIds, that.childIds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Meta meta = (Meta) o;
    return trace == meta.trace &&
            tombstone == meta.tombstone &&
            Objects.equals(globalTime, meta.globalTime) &&
            Arrays.equals(childIds, meta.childIds);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(globalTime, trace, tombstone);
    result = 31 * result + Arrays.hashCode(childIds);
    return result;
  }

  @Override
  public String toString() {
    return "(" + globalTime + ", " + Arrays.toString(childIds) + ", " + trace + (tombstone ? ", tombstone" : "") + ')';
  }

  public static class ArrayComparator implements Comparator<int[]> {
    @Override
    public int compare(int[] o1, int[] o2) {
      for (int i = 0; i < Math.min(o1.length, o2.length); ++i) {
        if (o1[i] != o2[i]) {
          return Long.compare(o1[i], o2[i]);
        }
      }
      return Integer.compare(o1.length, o2.length);
    }
  }
}
