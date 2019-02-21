  package com.spbsu.flamestream.example.bl.tfidf.model;


import com.spbsu.flamestream.example.bl.tfidf.TextUtils;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier.Topic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TfIdfObject implements DocContainer {
  private final int number;
  private final Map<String, Integer> counts;
  private final Map<String, Integer> idfCounts;
  private final String docName;
  private final String partitioning;
  private final int idfCardinality;
  private final int trainNumber;
  private final Topic[] topics;

  public Set<String> tfKeys() {
    return counts.keySet();
  }

  public int tfCount(String key) {
    return counts.get(key);
  }

  public int idfCount(String key) {
    return idfCounts.get(key);
  }

  private TfIdfObject(String docName, List<String> words, String partitioning, int number, Topic[] topics, int trainNumber) {
    this.docName = docName;
    this.number = number;
    this.partitioning = partitioning;
    counts = new HashMap();
    idfCounts = new HashMap();
    for (String s: words) {
      counts.put(s, counts.getOrDefault(s, 0) + 1);
    }
    idfCardinality = 0;
    this.topics = topics;
    this.trainNumber = trainNumber;
  }

  public static TfIdfObject ofText(TextDocument textDocument) {
    return new TfIdfObject(textDocument.name(), TextUtils.words(textDocument.content()), textDocument.partitioning(),
            textDocument.number(), textDocument.topics(), textDocument.trainNumber());
  }

  public TfIdfObject(TfIdfObject tfIdfObject, Map<String, Integer> idf) {
    this(tfIdfObject.docName, tfIdfObject.counts, idf, tfIdfObject.partitioning, tfIdfObject.number, tfIdfObject.topics, tfIdfObject.trainNumber);
  }

  private TfIdfObject(String docName, Map counts, Map idfCounts, String partitioning, int number, Topic[] topics, int trainNumber) {
    this.docName = docName;
    this.counts = counts;
    this.idfCounts = idfCounts;
    this.partitioning = partitioning;
    idfCardinality = 0;
    this.number = number;
    this.topics = topics;
    this.trainNumber = trainNumber;
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
  public int idfCardinality() {
    return idfCardinality;
  }


  public int trainNumber() {
    return trainNumber;
  }

  public Topic[] topics() {
    return topics;
  }

  @Override
  public String toString() {
    return String.format("<TFO> doc hash: %d, doc: %s, idf: %s, words: %s", docName.hashCode(), docName, idfCounts, counts);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TfIdfObject otherTfIdfObject = (TfIdfObject) o;
    return Objects.equals(counts, otherTfIdfObject.counts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(counts.hashCode());
  }
}
