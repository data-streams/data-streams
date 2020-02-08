package com.spbsu.flamestream.core.data.meta;

import com.spbsu.flamestream.core.TrackingComponent;

import java.util.Comparator;
import java.util.Objects;

public final class GlobalTime implements Comparable<GlobalTime> {
  public static final GlobalTime MIN = new GlobalTime(Long.MIN_VALUE, EdgeId.Min.INSTANCE);
  public static final GlobalTime MAX = new GlobalTime(Long.MAX_VALUE, EdgeId.Max.INSTANCE, Integer.MAX_VALUE);
  //Inner representation is a subject for a discussion and/or an optimization

  private static final Comparator<GlobalTime> NATURAL_ORDER = Comparator
          .comparing(GlobalTime::trackingComponent)
          .thenComparingLong(GlobalTime::time)
          .thenComparing(GlobalTime::frontId);

  private final long time;
  private final EdgeId frontId;
  private final int trackingComponent;

  public GlobalTime(long time, EdgeId frontId) {
    this(time, frontId, 0);
  }

  public GlobalTime(long time, EdgeId frontId, int trackingComponent) {
    this.time = time;
    this.frontId = frontId;
    this.trackingComponent = trackingComponent;
  }

  public long time() {
    return time;
  }

  public int trackingComponent() { return trackingComponent; }

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
            trackingComponent == that.trackingComponent &&
            Objects.equals(frontId, that.frontId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, frontId);
  }

  @Override
  public String toString() {
    return String.format("(%07d, %s, %s)", time, frontId, trackingComponent);
  }
}
