package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.Vec;

public interface BiClassifierOptimizer {
  Vec optimizeWeights(Mx trainingSet, int[] isCorrect, Vec prevWeights);
}
