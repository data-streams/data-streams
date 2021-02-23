package com.spbsu.flamestream.core.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class HashGroup {
  public static HashGroup EMPTY = new HashGroup(Collections.emptySet());

  private final Set<HashUnit> units;

  @JsonCreator
  public HashGroup(@JsonProperty("units") Set<HashUnit> units) {
    this.units = units;
  }

  @JsonProperty
  public Set<HashUnit> units() {
    return units;
  }

  @JsonIgnore
  public boolean covers(int hash) {
    for (final HashUnit hashUnit : units) {
      if (hashUnit.covers(hash)) {
        return true;
      }
    }
    return false;
  }

  public boolean isEmpty() {
    for (final HashUnit unit : units) {
      if (!unit.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HashGroup hashGroup = (HashGroup) o;
    return Objects.equals(units, hashGroup.units);
  }

  @Override
  public String toString() {
    return "{" + units.stream().map(Objects::toString).collect(Collectors.joining(", ")) + "}";
  }

  @Override
  public int hashCode() {
    return Objects.hash(units);
  }
}
