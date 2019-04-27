package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class Classifier {
  private final TopicsPredictor predictor;

  public Classifier(TopicsPredictor predictor) {
    this.predictor = predictor;
  }

  public Prediction apply(TfIdfObject tfIdfObject) {
    final Map<String, Double> tfIdf = new HashMap<>();
    { //normalized tf-idf
      double squareSum = 0.0;
      for (String word : tfIdfObject.words()) {
        double tfIdfValue =
                tfIdfObject.tf(word) * Math.log((double) tfIdfObject.number() / (double) tfIdfObject.idf(word))
                        + 1;
        squareSum += (tfIdfValue * tfIdfValue);
        tfIdf.put(word, tfIdfValue);
      }
      final double norm = Math.sqrt(squareSum);
      tfIdf.forEach((s, v) -> tfIdf.put(s, v / norm));
    }

    final Document document = new Document(tfIdf);
    return new Prediction(tfIdfObject, predictor.predict(document));
  }

  public void init() {
    predictor.init();
  }
}
