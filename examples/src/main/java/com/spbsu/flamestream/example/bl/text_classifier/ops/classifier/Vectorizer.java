package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

import com.expleague.commons.math.vectors.Vec;

public interface Vectorizer {
  default void init() {
  }

  Vec vectorize(Document document);

  int dim();
}
