package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;

public class FTRLState implements ModelState {
  private final Mx weights;
  private final Mx zed;
  private final Mx norm;

  public FTRLState(int rows, int columns) {
    weights = new SparseMx(rows, columns);
    zed = new VecBasedMx(rows, columns);
    norm = new VecBasedMx(rows, columns);
  }

  public Mx weights() {
    return weights;
  }

  public Mx zed() {
    return zed;
  }

  public Mx norm() {
    return norm;
  }
}
