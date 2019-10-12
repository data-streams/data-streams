package com.spbsu.flamestream.core.data.meta;

import java.util.Comparator;
import java.util.Objects;

public final class GlobalTime implements Comparable<GlobalTime> {
  public static final GlobalTime MIN = new GlobalTime(Long.MIN_VALUE, EdgeId.Min.INSTANCE);
  public static final GlobalTime MAX = new GlobalTime(Long.MAX_VALUE, EdgeId.Max.INSTANCE, Integer.MAX_VALUE);
  //Inner representation is a subject for a discussion and/or an optimization

  private static final Comparator<GlobalTime> NATURAL_ORDER = Comparator
          .comparingInt(GlobalTime::getVertexIndex)
          .thenComparingLong(GlobalTime::time)
          .thenComparing(GlobalTime::frontId);

  private final long time;
  private final EdgeId frontId;
  private final int vertexIndex;

  public GlobalTime(long time, EdgeId frontId) {
    this(time, frontId, 0);
  }

  public GlobalTime(long time, EdgeId frontId, int vertexIndex) {
    this.time = time;
    this.frontId = frontId;
    this.vertexIndex = vertexIndex;
  }

  public long time() {
    return time;
  }

  public int getVertexIndex() {
    return vertexIndex;
  }

  public EdgeId frontId() {
    return frontId;
  }

  @Override
  public int compareTo(GlobalTime that) {
    return NATURAL_ORDER.compare(this, that);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GlobalTime that = (GlobalTime) o;
    return time == that.time &&
            vertexIndex == that.vertexIndex &&
            Objects.equals(frontId, that.frontId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, frontId);
  }

  @Override
  public String toString() {
    return String.format("(%07d, %s, %d)", time, frontId, vertexIndex);
  }
}
