package com.spbsu.flamestream.runtime.config;

import com.spbsu.flamestream.runtime.master.acker.LocalAcker;

public class SystemConfig {
  public enum Acking {
    DISABLED,
    CENTRALIZED,
    DISTRIBUTED;
  }

  private final int maxElementsInGraph;
  private final int millisBetweenCommits;
  private final long defaultMinimalTime;
  private final Acking acking;
  private final boolean barrierDisabled;
  private final LocalAcker.Builder localAckerBuilder;
  private final int ackerWindow;

  public SystemConfig(
          int maxElementsInGraph,
          int millisBetweenCommits,
          long defaultMinimalTime,
          Acking acking,
          boolean barrierDisabled,
          LocalAcker.Builder localAckerBuilder,
          int ackerWindow
  ) {
    if (acking == SystemConfig.Acking.DISABLED && !barrierDisabled)
      throw new IllegalArgumentException("barrier should be disabled when acking is");
    this.maxElementsInGraph = maxElementsInGraph;
    this.millisBetweenCommits = millisBetweenCommits;
    this.defaultMinimalTime = defaultMinimalTime;
    this.acking = acking;
    this.barrierDisabled = barrierDisabled;
    this.localAckerBuilder = localAckerBuilder;
    this.ackerWindow = ackerWindow;
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

  public Acking acking() {
    return this.acking;
  }

  public int ackerWindow() {
    return ackerWindow;
  }

  @Override
  public String toString() {
    return "SystemConfig{maxElementsInGraph=" + maxElementsInGraph + '}';
  }

  public boolean barrierIsDisabled() {
    return barrierDisabled;
  }

  public LocalAcker.Builder getLocalAckerBuilder() {
    return localAckerBuilder;
  }
}
