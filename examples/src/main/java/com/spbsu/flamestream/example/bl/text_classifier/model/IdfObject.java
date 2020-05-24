package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IdfObject implements DocContainer {
  private final Map<String, Integer> counts = new HashMap<>();
  private final String docName;
  private final String partitioning;
  private final boolean labeled;

  public IdfObject(Set<WordCounter> counters) {
    counters.forEach(wordCounter -> counts.put(wordCounter.word(), wordCounter.count()));
    labeled = counters.stream().findFirst().get().labeled();
    docName = counters.iterator().next().document();
    partitioning = counters.iterator().next().partitioning();
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
  public boolean labeled() {
    return labeled;
  }

  @Override
  public String toString() {
    return String.format("<IDFO> %s", counts);
  }

  public Map<String, Integer> counts() {
    return counts;
  }
}
