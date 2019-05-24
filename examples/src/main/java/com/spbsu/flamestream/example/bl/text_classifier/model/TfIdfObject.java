package com.spbsu.flamestream.example.bl.text_classifier.model;


import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

import java.util.Map;
import java.util.Set;

public class TfIdfObject implements DocContainer, ClassifierInput {
  private final int number;
  private final Map<String, Integer> tf;
  private final Map<String, Integer> idf;
  private final String docName;
  private final String partitioning;
  private final String label;

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
    this.label = tfObject.label();
  }

  public String label() {
    return label;
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
