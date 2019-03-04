package com.spbsu.flamestream.example.benchmark.validators.classifier;

import com.spbsu.flamestream.example.benchmark.BenchValidator;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;

public class Lenta1100Validator implements BenchValidator<Prediction> {
  @Override
  public int inputLimit() {
    return 1095;
  }

  @Override
  public int expectedOutputSize() {
    return 1095;
  }

  @Override
  public void stop() {
    // TODO: 04.03.19 implement me
  }

  @Override
  public void accept(Prediction prediction) {
    // TODO: 04.03.19 implement me
  }
}
