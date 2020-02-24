package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.TrackingComponent;

public class LabelMarkers<L> extends Graph.Vertex.Stub {
  public final LabelSpawn<?, L> labelSpawn;
  public final TrackingComponent trackingComponent;

  public LabelMarkers(LabelSpawn<?, L> labelSpawn, TrackingComponent trackingComponent) {
    this.labelSpawn = labelSpawn;
    this.trackingComponent = trackingComponent;
  }
}
