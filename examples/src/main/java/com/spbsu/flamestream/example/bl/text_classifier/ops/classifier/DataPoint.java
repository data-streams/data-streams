package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

import com.expleague.commons.math.vectors.Vec;

public class DataPoint {
  private final Vec features;
  private final String label;

  public DataPoint(Vec features, String label) {
    this.features = features;
    this.label = label;
  }

  public String getLabel() {
    return label;
  }

  public Vec getFeatures() {
    return features;
  }
}
