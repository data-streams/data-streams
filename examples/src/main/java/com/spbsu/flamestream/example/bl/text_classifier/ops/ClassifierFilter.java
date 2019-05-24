package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.expleague.commons.math.vectors.Vec;
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
import java.util.function.Function;
import java.util.stream.Stream;

public class ClassifierFilter implements Function<List<ClassifierInput>, Stream<ClassifierOutput>> {
  private final static Logger LOG = LoggerFactory.getLogger(ClassifierFilter.class);
  private final Vectorizer vectorizer;
  private final OnlineModel model;

  public ClassifierFilter(Vectorizer vectorizer, OnlineModel model) {
    this.vectorizer = vectorizer;
    this.model = model;
  }

  public void init() {
    vectorizer.init();
  }

  @Override
  public Stream<ClassifierOutput> apply(List<ClassifierInput> input) {
    if (input.size() == 1) {
      final TfIdfObject tfIdfObject = (TfIdfObject) input.get(0);
      if (tfIdfObject.label() == null) {
        LOG.warn("Cannot process doc: {}. Empty model.", tfIdfObject.document());
        return Stream.empty();
      }
      return Stream.of(
              new Prediction(tfIdfObject, new Topic[]{}),
              new Classifier(vectorizer, model).initialState(tfIdfObject)
      );
    }

    if (input.get(0) instanceof ClassifierState) {
      final TfIdfObject tfIdfObject = (TfIdfObject) input.get(1);
      ClassifierState classifierState = (ClassifierState) input.get(0);
      if (tfIdfObject.label() == null) {
        return Stream.of(new Prediction(
                tfIdfObject,
                new Classifier(vectorizer, model).topics(classifierState, tfIdfObject)
        ), classifierState);
      } else {
        return Stream.of(
                new Prediction(tfIdfObject, new Topic[]{}),
                new Classifier(vectorizer, model).newState(classifierState, tfIdfObject)
        );
      }
    } else {
      return Stream.of();
    }
  }
}
