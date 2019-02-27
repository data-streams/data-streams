package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;


import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class Trainer implements Function<List<TfIdfObject>, Stream<Object>> {
  @Override
  public Stream<Object> apply(List<TfIdfObject> data) {
    System.out.println("data: " + data.size());
    data.stream().forEach(e -> System.out.println(e.trainNumber()));
    return Stream.of();
  }

}
