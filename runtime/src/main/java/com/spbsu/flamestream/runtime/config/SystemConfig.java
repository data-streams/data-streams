package com.spbsu.flamestream.runtime.config;

import com.spbsu.flamestream.runtime.master.acker.LocalAcker;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

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

    default Stream<HashGroup> hashGroups(Stream<String> ids) {
      return HashUnit.covering((int) ids.count()).map(Collections::singleton).map(HashGroup::new);
    }
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

  public static class Builder {
    private int maxElementsInGraph = 500;
    private int millisBetweenCommits = 100;
    private int defaultMinimalTime = 0;
    private boolean barrierDisabled = false;
    private LocalAcker.Builder localAckerBuilder = new LocalAcker.Builder();
    private int ackerWindow = 1;
    private SystemConfig.WorkersResourcesDistributor workersResourcesDistributor =
            SystemConfig.WorkersResourcesDistributor.DEFAULT_CENTRALIZED;

    public Builder maxElementsInGraph(int maxElementsInGraph) {
      this.maxElementsInGraph = maxElementsInGraph;
      return this;
    }

    public Builder millisBetweenCommits(int millisBetweenCommits) {
      this.millisBetweenCommits = millisBetweenCommits;
      return this;
    }

    public Builder defaultMinimalTime(int defaultMinimalTime) {
      this.defaultMinimalTime = defaultMinimalTime;
      return this;
    }

    public Builder barrierDisabled(boolean barrierDisabled) {
      this.barrierDisabled = barrierDisabled;
      return this;
    }

    public Builder localAckerBuilder(LocalAcker.Builder localAckerBuilder) {
      this.localAckerBuilder = localAckerBuilder;
      return this;
    }

    public Builder ackerWindow(int window) {
      this.ackerWindow = window;
      return this;
    }

    public Builder workersResourcesDistributor(SystemConfig.WorkersResourcesDistributor workersResourcesDistributor) {
      this.workersResourcesDistributor = workersResourcesDistributor;
      return this;
    }

    public SystemConfig build() {
      return new SystemConfig(
              maxElementsInGraph,
              millisBetweenCommits,
              defaultMinimalTime,
              barrierDisabled,
              localAckerBuilder,
              ackerWindow,
              workersResourcesDistributor
      );
    }
  }
}
