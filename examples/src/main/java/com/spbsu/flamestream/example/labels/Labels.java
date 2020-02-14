package com.spbsu.flamestream.example.labels;

import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class Labels {
  private final Map<Class<?>, Label<?>> all;

  public static class Entry<Value> {
    public final Class<Value> aClass;
    public final Label<? extends Value> value;

    public Entry(Class<Value> aClass, Label<? extends Value> value) {
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

  private Labels(Map<Class<?>, Label<?>> all) {
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

  public <Value> Labels added(Class<Value> valueClass, Label<Value> value) {
    final HashMap<Class<?>, Label<?>> added = new HashMap<>(all);
    added.put(valueClass, value);
    return new Labels(added);
  }

  @Nullable <Value> Label<Value> get(Class<Value> aClass) {
    return (Label<Value>) all.get(aClass);
  }

  @Nullable <Value> Entry<Value> entry(Class<Value> aClass) {
    return new Entry<>(aClass, (Label<Value>) all.get(aClass));
  }

  public boolean hasAll(Set<Class<?>> classes) {
    for (final Class<?> aClass : classes) {
      if (!all.containsKey(aClass))
        return false;
    }
    return true;
  }
}
