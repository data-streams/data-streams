package com.spbsu.flamestream.example.bl.text_classifier.model;

public class TextDocument {
  private final String name;
  private final String content;
  private final String partitioning;
  private final int number;
  private final String topic;

  public TextDocument(String name, String content, String partitioning, int number) {
    this.name = name;
    this.content = content;
    this.partitioning = partitioning;
    this.number = number;
    this.topic = null;
  }

  public TextDocument(String name, String content, String partitioning, int number, String topic) {
    this.name = name;
    this.content = content;
    this.partitioning = partitioning;
    this.number = number;
    this.topic = topic;
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

  @Override
  public String toString() {
    return String.format("%s: >%s<", name, content);
  }

  public String topic() {
    return this.topic;
  }
}
