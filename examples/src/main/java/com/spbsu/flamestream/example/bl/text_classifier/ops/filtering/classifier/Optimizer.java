package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;

public interface Optimizer {
  Mx optimizeWeights(SparseMx trainingSet, String[] correctTopics, Mx prevWeights);
}
