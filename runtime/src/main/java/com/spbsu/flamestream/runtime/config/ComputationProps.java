package com.spbsu.flamestream.runtime.config;

import java.util.Map;

public class ComputationProps {
  private final int maxElementsInGraph;
  private final boolean barrierIsDisabled;
  private final Map<String, HashGroup> hashGroups;
  private final int ackerVerticesNumber;

  public ComputationProps(
          Map<String, HashGroup> hashGroups,
          int maxElementsInGraph,
          boolean barrierIsDisabled,
          int ackerVerticesNumber
  ) {
    this.hashGroups = hashGroups;
    this.maxElementsInGraph = maxElementsInGraph;
    this.barrierIsDisabled = barrierIsDisabled;
    this.ackerVerticesNumber = ackerVerticesNumber;
  }

  public Map<String, HashGroup> hashGroups() {
    return hashGroups;
  }

  public int maxElementsInGraph() {
    return this.maxElementsInGraph;
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

  public int getAckerVerticesNumber() {
    return ackerVerticesNumber;
  }
}
