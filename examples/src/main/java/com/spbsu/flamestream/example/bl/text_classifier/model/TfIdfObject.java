package com.spbsu.flamestream.example.bl.text_classifier.model;


import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

import java.util.Map;
import java.util.Set;

public class TfIdfObject implements DocContainer {
  private final long number;
  private final Map<String, Integer> tf;
  private final Map<String, Integer> idf;
  private final String docName;
  private final String partitioning;

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
  }

  @Override
  public String document() {
    return docName;
  }

  public long number() {
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
