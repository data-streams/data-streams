package com.spbsu.flamestream.runtime.config;

public class SystemConfig {
  private final int maxElementsInGraph;
  private final int millisBetweenCommits;
  private final long defaultMinimalTime;
  private final boolean distributedAcker;
  private final boolean barrierDisabled;

  public SystemConfig(
          int maxElementsInGraph,
          int millisBetweenCommits,
          long defaultMinimalTime,
          boolean distributedAcker,
          boolean barrierDisabled
  ) {
    this.maxElementsInGraph = maxElementsInGraph;
    this.millisBetweenCommits = millisBetweenCommits;
    this.defaultMinimalTime = defaultMinimalTime;
    this.distributedAcker = distributedAcker;
    this.barrierDisabled = barrierDisabled;
  }

  public long defaultMinimalTime() {
    return defaultMinimalTime;
  }

  public int maxElementsInGraph() {
    return maxElementsInGraph;
  }

  public int millisBetweenCommits() {
    return this.millisBetweenCommits;
  }

  public boolean distributedAcker() {
    return this.distributedAcker;
  }

  @Override
  public String toString() {
    return "SystemConfig{maxElementsInGraph=" + maxElementsInGraph + '}';
  }

  public boolean barrierIsDisabled() {
    return barrierDisabled;
  }
}
