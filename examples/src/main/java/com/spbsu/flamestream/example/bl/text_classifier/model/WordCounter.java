package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.WordContainer;

import java.util.Objects;

public class WordCounter implements WordContainer, DocContainer {
  private final WordEntry wordEntry;
  private final int count;

  public WordCounter(WordEntry wordEntry, int count) {
    this.wordEntry = wordEntry;
    this.count = count;
  }

  @Override
  public String word() {
    return wordEntry.word();
  }

  @Override
  public String document() {
    return wordEntry.document();
  }

  @Override
  public String partitioning() {
    return wordEntry.partitioning();
  }

  @Override
  public boolean labeled() {
    return wordEntry.labeled();
  }

  public int idfCardinality() {
    return wordEntry.idfCardinality();
  }

  public int count() {
    return count;
  }

  @Override
  public String toString() {
    return String.format("<WC> %s: %d", wordEntry, count);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WordCounter that = (WordCounter) o;
    return Objects.equals(wordEntry.word(), that.wordEntry.word());
  }

  @Override
  public int hashCode() {
    return Objects.hash(wordEntry.word());
  }
}
