package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.TrackingComponent;
import org.jetbrains.annotations.Nullable;

public class LabelMarkers extends HashingVertexStub {
  public final TrackingComponent trackingComponent;
  public final HashFunction hashFunction;

  @Override
  @Nullable
  public HashFunction hash() {
    return hashFunction;
  }

  public LabelMarkers(TrackingComponent trackingComponent) {
    this(trackingComponent, null);
  }

  public LabelMarkers(TrackingComponent trackingComponent, HashFunction hashFunction) {
    this.trackingComponent = trackingComponent;
    this.hashFunction = hashFunction;
  }
}
