package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

import com.expleague.commons.math.vectors.Vec;

public interface TopicsPredictor {
  default void init() {
  }

  Topic[] predict(Vec vec);
}
