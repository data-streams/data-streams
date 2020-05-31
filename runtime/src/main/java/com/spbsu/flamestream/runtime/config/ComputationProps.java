package com.spbsu.flamestream.runtime.config;

import com.spbsu.flamestream.core.graph.HashGroup;

import java.util.Map;

public class ComputationProps {
  private final int maxElementsInGraph;
  private final boolean barrierIsDisabled;
  private final Map<String, HashGroup> hashGroups;
  private final int partitions;

  public ComputationProps(
          Map<String, HashGroup> hashGroups, int maxElementsInGraph, boolean barrierIsDisabled, int partitions
  ) {
    this.hashGroups = hashGroups;
    this.maxElementsInGraph = maxElementsInGraph;
    this.barrierIsDisabled = barrierIsDisabled;
    this.partitions = partitions;
  }

  public Map<String, HashGroup> hashGroups() {
    return hashGroups;
  }

  public int maxElementsInGraph() {
    return this.maxElementsInGraph;
  }

  public int partitions() {
    return partitions;
  }

  @Override
  public String toString() {
    return "ComputationLayout{" +
            "hashGroups=" + hashGroups +
            '}';
  }

  public boolean barrierIsDisabled() {
    return barrierIsDisabled;
  }
}
