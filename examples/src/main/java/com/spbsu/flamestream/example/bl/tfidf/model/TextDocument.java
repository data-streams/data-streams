package com.spbsu.flamestream.example.bl.tfidf.model;

import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier.Topic;

public class TextDocument {
  private final String name;
  private final String content;
  private final String partitioning;
  private final Topic[] topics;
  private final int number;
  private final int trainNumber;

  public TextDocument(String name, String content, String partitioning, int number) {
    this.name = name;
    this.content = content;
    this.partitioning = partitioning;
    this.number = number;
    this.topics = null;
    this.trainNumber = 0;
  }

  public TextDocument(String name, String content, String partitioning, int number, Topic[] topics, int trainNumber) {
    this.name = name;
    this.content = content;
    this.partitioning = partitioning;
    this.number = number;
    this.topics = topics;
    this.trainNumber = trainNumber;
  }

  public String name() {
    return name;
  }

  public String content() {
    return content;
  }

  public String partitioning() {
    return partitioning;
  }

  public int number() {
    return number;
  }

  public int trainNumber() {
    return trainNumber;
  }

  public Topic[] topics() {
    return topics;
  }

  @Override
  public String toString() {
    return String.format("%s: >%s<", name, content);
  }
}
