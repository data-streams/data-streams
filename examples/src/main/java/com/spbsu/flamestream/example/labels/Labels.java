package com.spbsu.flamestream.example.labels;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Labels {
  private final Map<Class<? extends Label>, ? extends Label> all;

  public static class Entry<Value extends Label> {
    public final Class<Value> aClass;
    public final Value value;

    public Entry(Class<Value> aClass, Value value) {
      this.aClass = aClass;
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      return obj.equals(aClass);
    }

    @Override
    public int hashCode() {
      return aClass.hashCode();
    }
  }

  public static final Labels EMPTY = new Labels(Collections.emptySet());

  public Labels(Set<Entry<?>> entries) {
    all = entries.stream().collect(Collectors.toMap(entry -> entry.aClass, entry -> entry.value));
  }

  <Value extends Label> Value get(Class<Value> aClass) {
    return Objects.requireNonNull((Value) all.get(aClass));
  }
}
