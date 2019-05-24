package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

public class TextDocument implements DocContainer {
  private final String name;
  private final String content;
  private final String partitioning;
  private final String label;
  private final int number;

  public TextDocument(String name, String content, String partitioning, int number, String label) {
    this.name = name;
    this.content = content;
    this.partitioning = partitioning;
    this.label = label;
    this.number = number;
  }

  public String label() {
    return label;
  }

  public String name() {
    return name;
  }

  public String content() {
    return content;
  }

  @Override
  public String document() {
    return name;
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
}
