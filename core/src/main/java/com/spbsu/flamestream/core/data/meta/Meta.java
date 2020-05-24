package com.spbsu.flamestream.core.data.meta;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public class Meta implements Comparable<Meta> {
  private static final int[] EMPTY_ARRAY = new int[0];

  public static final Comparator<Meta> NATURAL_ORDER = Comparator
          .comparing(Meta::globalTime)
          .thenComparing(Meta::childIds, Arrays::compare)
          .thenComparing(Meta::trace)
          .thenComparing(Meta::isTombstone);


  private final GlobalTime globalTime;
  private final int[] childIds;
  private final long trace;
  private final boolean tombstone;
  private final Labels labels;

  public Meta(GlobalTime globalTime) {
    this.globalTime = globalTime;
    this.childIds = EMPTY_ARRAY;
    this.tombstone = false;
    this.trace = 0;
    labels = Labels.EMPTY;
  }

  public Meta(Meta previous, long physicalId, boolean tombstone) {
    this(previous, physicalId, tombstone, previous.labels);
  }

  public Meta(Meta previous, long physicalId, boolean tombstone, Labels labels) {
    this.globalTime = previous.globalTime();
    this.childIds = previous.childIds;
    this.tombstone = tombstone;
    this.trace = previous.trace ^ physicalId;
    this.labels = labels;
  }

  public Meta(Meta previous, long physicalId, int childId) {
    this.globalTime = previous.globalTime();
    this.childIds = Arrays.copyOf(previous.childIds, previous.childIds.length + 1);
    childIds[childIds.length - 1] = childId;
    this.tombstone = previous.tombstone;
    this.trace = previous.trace ^ physicalId;
    this.labels = previous.labels;
  }

  public Meta(Meta previous, long physicalId, int childId, Labels labels) {
    this.globalTime = previous.globalTime();
    this.childIds = Arrays.copyOf(previous.childIds, previous.childIds.length + 1);
    childIds[childIds.length - 1] = childId;
    this.tombstone = previous.tombstone;
    this.trace = previous.trace ^ physicalId;
    this.labels = labels;
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

  public int childIdsLength() {
    return childIds.length;
  }

  public Labels labels() {
    return labels;
  }

  @Override
  public int compareTo(@NotNull Meta that) {
    return NATURAL_ORDER.compare(this, that);
  }

  public int comparePrefixedChildIds(int thisPrefix, Meta that, int thatPrefix) {
    return Comparator.comparing(
            Meta::childIds,
            (a, b) -> Arrays.compare(a, 0, thisPrefix, b, 0, thatPrefix)
    ).compare(this, that);
  }

  public boolean childIdsArePrefixTo(Meta that) {
    return Arrays.mismatch(childIds, that.childIds) == childIds.length;
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
}
