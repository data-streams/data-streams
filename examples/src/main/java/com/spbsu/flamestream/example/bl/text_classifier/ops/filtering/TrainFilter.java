package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;


import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TrainInput;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class TrainFilter implements Function<List<TfIdfObject>, Stream<TrainInput>> {
  @Override
  public Stream<TrainInput> apply(List<TfIdfObject> data) {
      return Stream.of(new TrainInput(data));
  }
}
