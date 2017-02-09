package com.spbsu.datastream.example.invertedindex.models;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordIndex implements WordContainer {
  @JsonProperty
  private final String word;
  @JsonProperty
  private final long[] positions;

  public WordIndex(String word, long[] positions) {
    this.word = word;
    this.positions = positions;
  }
  @Override
  public String word() {
    return word;
  }

  public long[] positions() {
    return positions;
  }
}
