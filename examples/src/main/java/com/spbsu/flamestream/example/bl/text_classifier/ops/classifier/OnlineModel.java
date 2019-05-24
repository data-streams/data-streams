package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

import com.expleague.commons.math.vectors.Vec;

public interface OnlineModel {
  ModelState step(DataPoint trainingPoint, ModelState prevState);

  Topic[] predict(ModelState state, Vec vec);

  int classes();
}
