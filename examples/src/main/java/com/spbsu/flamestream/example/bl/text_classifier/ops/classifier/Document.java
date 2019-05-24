package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

import java.util.Map;

public class Document {
  private final Map<String, Double> tfIdf;

  public Document(Map<String, Double> tfIdf) {
    this.tfIdf = tfIdf;
  }

  Map<String, Double> tfIdf() {
    return tfIdf;
  }
}