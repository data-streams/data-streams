package com.spbsu.flamestream.example.bl.text_classifier.model;


import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;

import java.util.Map;
import java.util.Set;

public class TfIdfObject implements DocContainer {
  private final int number;
  private final Map<String, Integer> tf;
  private final Map<String, Integer> idf;
  private final String docName;
  private final String partitioning;
  private final int trainNumber;
  private final Topic[] topics;

  public Set<String> words() {
    return tf.keySet();
  }

  public int tf(String key) {
    return tf.get(key);
  }

  public int idf(String key) {
    return idf.get(key);
  }

  public TfIdfObject(TfObject tfObject, IdfObject idfObject) {
    this.docName = tfObject.document();
    this.tf = tfObject.counts();
    this.idf = idfObject.counts();
    this.partitioning = tfObject.partitioning();
    this.number = tfObject.number();
    this.topics = tfObject.topics();
    this.trainNumber = tfObject.trainNumber();
  }

  @Override
  public String document() {
    return docName;
  }

  public int number() {
    return number;
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

  @Override
  public String toString() {
    return String.format(
            "<TFO> doc hash: %d, doc: %s, idf: %s, words: %s",
            docName.hashCode(),
            docName,
            idf,
            tf
    );
  }
}
