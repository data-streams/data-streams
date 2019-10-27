package com.spbsu.flamestream.runtime.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public class HashUnit {
  private final String id;
  private final int from;
  private final int to;

  @JsonCreator
  public HashUnit(@JsonProperty("from") int from, @JsonProperty("to") int to) {
    this.from = from;
    this.to = to;
    id = from + "_" + to;
  }

  @JsonProperty("from")
  public int from() {
    return from;
  }

  @JsonProperty("to")
  public int to() {
    return to;
  }

  @JsonProperty("id")
  public String id() {
    return id;
  }

  @Override
  public String toString() {
    return "HashUnit{" +
            "from=" + from +
            ", to=" + to +
            '}';
  }

  public static Stream<HashUnit> covering(int n) {
    final Set<HashUnit> result = new HashSet<>();
    if (n == 0) {
      return result.stream();
    }
    final int step = (int) (((long) Integer.MAX_VALUE - Integer.MIN_VALUE) / n);
    long left = Integer.MIN_VALUE;
    long right = left + step;
    for (int i = 0; i < n; ++i) {
      result.add(new HashUnit((int) left, (int) right));
      left += step;
      right = Math.min(Integer.MAX_VALUE, right + step);
    }
    return result.stream();
  }

  @JsonIgnore
  public boolean covers(int hash) {
    return hash >= from && hash < to;
  }

  @JsonIgnore
  public boolean isEmpty() {
    return from >= to;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HashUnit hashUnit = (HashUnit) o;
    return Objects.equals(id, hashUnit.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
