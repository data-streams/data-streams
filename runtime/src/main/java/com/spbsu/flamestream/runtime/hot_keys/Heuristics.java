package com.spbsu.flamestream.runtime.hot_keys;

import java.util.Arrays;
import java.util.function.ToIntFunction;

public class Heuristics {
  final double minLoad;
  final double maxSkew;

  public Heuristics(double minLoad, double maxSkew) {
    if (minLoad < 0) {
      throw new IllegalArgumentException();
    }
    if (maxSkew < 1) {
      throw new IllegalArgumentException();
    }
    this.minLoad = minLoad;
    this.maxSkew = maxSkew;
  }

  interface Partitions<Key> {
    int size();

    int map(Key key);
  }

  public <Key> boolean anyProblems(KeyFrequency<Key> keyFrequency, Partitions<Key> partitioner) {
    double[] partitionLoad = new double[partitioner.size()];
    keyFrequency.forEach((key, load) -> partitionLoad[partitioner.map(key)] += load);
    Arrays.sort(partitionLoad);
    return Double.max(minLoad, partitionLoad[partitionLoad.length - 2]) * maxSkew
            < partitionLoad[partitionLoad.length - 1];
  }
}
