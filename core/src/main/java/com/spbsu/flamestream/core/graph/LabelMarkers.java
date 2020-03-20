package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.TrackingComponent;

public class LabelMarkers extends Graph.Vertex.Stub {
  public final TrackingComponent trackingComponent;

  public LabelMarkers(TrackingComponent trackingComponent) {
    this.trackingComponent = trackingComponent;
  }
}
