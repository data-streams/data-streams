package com.spbsu.flamestream.example.labels;

import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Labels {
  private final Map<Class<? extends Label>, Label> all;

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

  private Labels(Map<Class<? extends Label>, Label> all) {
    this.all = all;
  }

  @Override
  public int hashCode() {
    return all.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    return obj.equals(all);
  }

  public <Value extends Label> Labels added(Class<Value> valueClass, Value value) {
    final HashMap<Class<? extends Label>, Label> added = new HashMap<>(all);
    added.put(valueClass, value);
    return new Labels(added);
  }

  @Nullable <Value extends Label> Value get(Class<Value> aClass) {
    return (Value) all.get(aClass);
  }
}
