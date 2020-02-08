package com.spbsu.flamestream.core;


import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

public class TrackingComponent implements Comparable<TrackingComponent> {
  public static final TrackingComponent DEFAULT = new TrackingComponent(0, Collections.emptyList());

  public final int index;
  public final List<TrackingComponent> inbound;

  public TrackingComponent(int index, List<TrackingComponent> inbound) {
    if (index < 0)
      throw new IllegalArgumentException(String.valueOf(index));
    for (final TrackingComponent trackingComponent : inbound) {
      if (index <= trackingComponent.index) {
        throw new IllegalArgumentException(trackingComponent.toString());
      }
    }
    this.inbound = Collections.unmodifiableList(inbound);
    this.index = index;
  }

  @Override
  public int compareTo(@NotNull TrackingComponent o) {
    return Integer.compare(index, o.index);
  }
}
