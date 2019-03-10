package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IdfObject implements DocContainer {
  private final Map<String, Integer> counts = new HashMap<>();
  private final String docName;
  private final String partitioning;

  public IdfObject(List<WordCounter> counters) {
    counters.forEach(wordCounter -> counts.put(wordCounter.word(), wordCounter.count()));
    docName = counters.get(0).document();
    partitioning = counters.get(0).partitioning();
  }

  @Override
  public String document() {
    return docName;
  }

  @Override
  public String partitioning() {
    return partitioning;
  }

  @Override
  public String toString() {
    return String.format("<IDFO> %s", counts);
  }

  public Map<String, Integer> counts() {
    return counts;
  }
}
