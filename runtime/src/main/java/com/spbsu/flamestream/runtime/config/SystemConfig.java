package com.spbsu.flamestream.runtime.config;

import com.spbsu.flamestream.runtime.master.acker.LocalAcker;

import java.util.Collections;
import java.util.List;

public class SystemConfig {
  public enum Acking {
    DISABLED,
    CENTRALIZED,
    DISTRIBUTED;
  }

  public interface WorkersResourcesDistributor {
    WorkersResourcesDistributor DEFAULT_DISABLED = ids -> Collections.emptyList();
    WorkersResourcesDistributor DEFAULT_CENTRALIZED = ids -> ids.subList(0, 1);
    WorkersResourcesDistributor DEFAULT_DISTRIBUTED = ids -> ids;

    default String master(List<String> ids) {
      return ids.get(0);
    }

    List<String> ackers(List<String> ids);
  }

  private final int maxElementsInGraph;
  private final int millisBetweenCommits;
  private final long defaultMinimalTime;
  private final boolean barrierDisabled;
  private final LocalAcker.Builder localAckerBuilder;
  private final int ackerWindow;
  public final WorkersResourcesDistributor workersResourcesDistributor;

  public SystemConfig(
          int maxElementsInGraph,
          int millisBetweenCommits,
          long defaultMinimalTime,
          boolean barrierDisabled,
          LocalAcker.Builder localAckerBuilder,
          int ackerWindow,
          WorkersResourcesDistributor workersResourcesDistributor
  ) {
    this.maxElementsInGraph = maxElementsInGraph;
    this.millisBetweenCommits = millisBetweenCommits;
    this.defaultMinimalTime = defaultMinimalTime;
    this.barrierDisabled = barrierDisabled;
    this.localAckerBuilder = localAckerBuilder;
    this.ackerWindow = ackerWindow;
    this.workersResourcesDistributor = workersResourcesDistributor;
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
