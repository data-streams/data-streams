package com.spbsu.flamestream.runtime.utils.collections;

import com.spbsu.flamestream.core.graph.HashUnit;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ListHashUnitMap<T> implements HashUnitMap<T> {
  private final Set<Map.Entry<HashUnit, T>> mapping;

  public ListHashUnitMap() {
    mapping = new HashSet<>();
  }

  @Override
  public T first() {
    return mapping.iterator().next().getValue();
  }

  @Override
  public T get(int key) {
    for (final Map.Entry<HashUnit, T> entry : mapping) {
      if (entry.getKey().covers(key)) {
        return entry.getValue();
      }
    }
    return null;
  }

  @Override
  public void putAll(Map<HashUnit, T> map) {
    mapping.addAll(map.entrySet());
  }

  @Override
  public void put(HashUnit unit, T value) {
    mapping.add(new Entry<>(unit, value));
  }

  @Override
  public String toString() {
    return mapping.toString();
  }

  @Override
  public Set<Map.Entry<HashUnit, T>> entrySet() {
    return mapping;
  }

  public static final class Entry<V> implements Map.Entry<HashUnit, V> {
    private final HashUnit unit;
    private V value;

    public Entry(HashUnit unit, V value) {
      this.unit = unit;
      this.value = value;
    }

    @Override
    public HashUnit getKey() {
      return unit;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      this.value = value;
      return value;
    }
  }
}
