package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class TfObject implements DocContainer {
  private final Map<String, Integer> counts;
  private final String docName;
  private final String partitioning;
  private final int number;
  private final int trainNumber;
  private final Topic[] topics;

  private TfObject(String docName, Stream<String> words, String partitioning, int number, Topic[] topics, int trainNumber) {
    this.docName = docName;
    this.number = number;
    this.partitioning = partitioning;
    counts = new HashMap<>();
    words.forEach(s -> counts.merge(s, 1, Integer::sum));
    this.topics = topics;
    this.trainNumber = trainNumber;
  }

  public static TfObject ofText(TextDocument textDocument) {
    return new TfObject(
            textDocument.name(),
            SklearnSgdPredictor.text2words(textDocument.content()),
            textDocument.partitioning(),
            textDocument.number(),
            textDocument.topics(),
            textDocument.trainNumber()
    );
  }

  public Map<String, Integer> counts() {
    return counts;
  }

  public int number() {
    return number;
  }

  @Override
  public String document() {
    return docName;
  }

  @Override
  public String partitioning() {
    return partitioning;
  }


  public int trainNumber() {
    return trainNumber;
  }

  public Topic[] topics() {
    return topics;
  }
}
