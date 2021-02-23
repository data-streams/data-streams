package com.spbsu.flamestream.runtime.master.acker;

import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class NodeTimes {
  private final HashMap<String, Long> all;

  public NodeTimes() {
    this.all = new HashMap<>();
  }

  private NodeTimes(HashMap<String, Long> all) {
    this.all = all;
  }

  public boolean greaterThanOrNotComparableTo(@NotNull NodeTimes other) {
    for (String id : all.keySet()) {
      if (all.getOrDefault(id, Long.MIN_VALUE) > other.all.getOrDefault(id, Long.MIN_VALUE)) {
        return true;
      }
    }
    return false;
  }

  public NodeTimes updated(String id, long time) {
    final HashMap<String, Long> all = new HashMap<>(this.all);
    final long currentTime = all.getOrDefault(id, Long.MIN_VALUE);
    if (currentTime > time)
      throw new RuntimeException("joba time is decreasing from " + currentTime + " to " + time);
    all.put(id, time);
    return new NodeTimes(all);
  }

  public NodeTimes min(NodeTimes other) {
    final Set<String> keys = new HashSet<>(all.keySet());
    keys.addAll(other.all.keySet());
    final HashMap<String, Long> min = new HashMap<>(all);
    for (String id : keys) {
      min.put(id, Long.min(all.getOrDefault(id, Long.MIN_VALUE), other.all.getOrDefault(id, Long.MIN_VALUE)));
    }
    return new NodeTimes(min);
  }

  public NodeTimes max(NodeTimes other) {
    HashMap<String, Long> max = new HashMap<>(this.all);
    for (String id : other.all.keySet()) {
      if (all.getOrDefault(id, Long.MIN_VALUE) < other.all.getOrDefault(id, Long.MIN_VALUE))
        max.put(id, other.all.get(id));
    }
    return new NodeTimes(max);
  }

  @Override
  public String toString() {
    return "NodeTimes{all = " + all + "}";
  }
}
