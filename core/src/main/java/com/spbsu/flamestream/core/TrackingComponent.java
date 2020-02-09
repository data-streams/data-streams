package com.spbsu.flamestream.core;


import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Set;

public class TrackingComponent implements Comparable<TrackingComponent> {
  public static final TrackingComponent DEFAULT = new TrackingComponent(0, Collections.emptySet());

  public final int index;
  public final Set<TrackingComponent> inbound;

  public TrackingComponent(int index, Set<TrackingComponent> inbound) {
    if (index < 0)
      throw new IllegalArgumentException(String.valueOf(index));
    for (final TrackingComponent trackingComponent : inbound) {
      if (index <= trackingComponent.index) {
        throw new IllegalArgumentException(trackingComponent.toString());
      }
    }
    this.inbound = Collections.unmodifiableSet(inbound);
    this.index = index;
  }

  @Override
  public int compareTo(@NotNull TrackingComponent o) {
    return Integer.compare(index, o.index);
  }
}
