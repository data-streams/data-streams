package com.spbsu.flamestream.example.bl.tfidf.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidf.model.Prediction;
import com.spbsu.flamestream.example.bl.tfidf.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier.SklearnSgdPredictor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class Classifier implements Function<TfIdfObject, Stream<Prediction>> {
  private final String cntVectorizerPath = "src/main/resources/cnt_vectorizer";
  private final String weightsPath = "src/main/resources/classifier_weights";
  private final SklearnSgdPredictor predictor = new SklearnSgdPredictor(cntVectorizerPath, weightsPath); // interface?

  @Override
  public Stream<Prediction> apply(TfIdfObject tfIdfObject) {
      final Document document = new Document(computeTfIdf(tfIdfObject));
      final Prediction result = new Prediction(tfIdfObject, predictor.predict(document));
      return Stream.of(result);
  }

  public void init() {
    predictor.init();
  }

  private Map<String, Double> computeTfIdf(TfIdfObject tfIdfObject) {
    final Map<String, Double> tfidf = new HashMap<>();

    double squareSum = 0.0;
    for (String word : tfIdfObject.tfKeys()) {
      double tfIdfValue =
              tfIdfObject.tfCount(word) * Math.log((double) tfIdfObject.number() / (double) tfIdfObject.idfCount(word)) + 1;
      squareSum += (tfIdfValue * tfIdfValue);
      tfidf.put(word, tfIdfValue);
    }

    final double norm = Math.sqrt(squareSum);
    tfidf.forEach((s, aDouble) -> tfidf.put(s, aDouble / norm));

    return tfidf;
  }

}
