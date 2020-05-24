package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.expleague.commons.math.vectors.Vec;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.example.bl.text_classifier.model.ClassifierState;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierOutput;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.OnlineModel;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Classifier {
  private final static Logger LOG = LoggerFactory.getLogger(Classifier.class);
  private final Vectorizer vectorizer;
  private final OnlineModel model;

  public Classifier(Vectorizer vectorizer, OnlineModel model) {
    this.vectorizer = vectorizer;
    this.model = model;
  }

  public void init() {
    vectorizer.init();
  }

  public ClassifierState step(ClassifierState classifierState, TfIdfObject tfIdfObject) {
    return new ClassifierState(model.step(
            new DataPoint(vectorize(tfIdfObject), tfIdfObject.label()),
            classifierState == null ? new FTRLState(model.classes(), vectorizer.dim()) : classifierState.getState()
    ));
  }

  public Topic[] predict(ClassifierState classifierState, TfIdfObject tfIdfObject) {
    return model.predict(classifierState.getState(), vectorize(tfIdfObject));
  }

  private Vec vectorize(TfIdfObject tfIdfObject) {
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
    return vectorizer.vectorize(new Document(tfIdf));
  }
}
