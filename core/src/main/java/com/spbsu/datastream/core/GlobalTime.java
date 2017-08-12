package com.spbsu.datastream.core;

import java.util.Comparator;
import java.util.Objects;

public final class GlobalTime implements Comparable<GlobalTime> {
  public static final GlobalTime MIN = new GlobalTime(Long.MIN_VALUE, -1);
  //Inner representation is a subject for a discussion and/or an optimization

  private static final Comparator<GlobalTime> NATURAL_ORDER = Comparator
          .comparingLong(GlobalTime::time)
          .thenComparingInt(GlobalTime::front);

  private final long time;

  private final int front;

  public GlobalTime(long time, int front) {
    this.time = time;
    this.front = front;
  }

  public long time() {
    return this.time;
  }

  public int front() {
    return this.front;
  }

  @Override
  public int compareTo(GlobalTime that) {
    return NATURAL_ORDER.compare(this, that);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    final GlobalTime that = (GlobalTime) o;
    return this.time == that.time &&
            Objects.equals(this.front, that.front);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.time, this.front);
  }

  @Override
  public String toString() {
    return "(" + this.time + ':' + this.front + ')';
  }
}
