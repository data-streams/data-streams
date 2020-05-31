package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.flamestream.core.graph.HashGroup;
import com.spbsu.flamestream.core.graph.HashUnit;
import com.spbsu.flamestream.runtime.master.acker.LocalAcker;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    default Map<String, HashGroup> hashGroups(List<String> ids) {
      final List<HashGroup> covering = HashUnit.covering(ids.size() - 1)
              .map(Collections::singleton)
              .map(HashGroup::new)
              .collect(Collectors.toList());
      final Map<String, HashGroup> ranges = new HashMap<>();
      ids.forEach(s -> ranges.put(s, s.equals(master(ids)) ? HashGroup.EMPTY : covering.remove(0)));
      assert covering.isEmpty();
      return ranges;
    }

    class Enumerated implements WorkersResourcesDistributor {
      private final String master;
      private final List<String> ackers;
      private final String prefix;

      public Enumerated(String prefix, int ackersNumber) {
        this.prefix = prefix;
        master = prefix + 0;
        ackers = IntStream.range(0, ackersNumber).mapToObj(index -> prefix + index).collect(Collectors.toList());
      }

      @Override
      public String master(List<String> ids) {
        return master;
      }

      @Override
      public List<String> ackers(List<String> ids) {
        return ackers;
      }

      @Override
      public Map<String, HashGroup> hashGroups(List<String> ids) {
        final HashGroup empty = new HashGroup(Collections.emptySet());
        final HashMap<String, HashGroup> all = new HashMap<>();
        final int skip = ackers.size();
        final int total = ids.size();
        final List<Set<HashUnit>> collect = HashUnit.covering(total - skip)
                .map(Collections::singleton)
                .collect(Collectors.toList());
        for (int index = 0; index < total; index++) {
          all.put(prefix + index, index < skip ? empty : new HashGroup(collect.remove(0)));
        }
        return all;
      }
    }
  }

  private final int maxElementsInGraph;
  private final int millisBetweenCommits;
  private final long defaultMinimalTime;
  private final boolean barrierDisabled;
  private final LocalAcker.Builder localAckerBuilder;
  private final int ackerWindow;
  public final WorkersResourcesDistributor workersResourcesDistributor;
  public final int partitions;

  public SystemConfig(
          int maxElementsInGraph,
          int millisBetweenCommits,
          long defaultMinimalTime,
          boolean barrierDisabled,
          LocalAcker.Builder localAckerBuilder,
          int ackerWindow,
          WorkersResourcesDistributor workersResourcesDistributor,
          int partitions
  ) {
    this.maxElementsInGraph = maxElementsInGraph;
    this.millisBetweenCommits = millisBetweenCommits;
    this.defaultMinimalTime = defaultMinimalTime;
    this.barrierDisabled = barrierDisabled;
    this.localAckerBuilder = localAckerBuilder;
    this.ackerWindow = ackerWindow;
    this.workersResourcesDistributor = workersResourcesDistributor;
    this.partitions = partitions;
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

  public Props localAckerProps(List<ActorRef> ackers, String nodeId) {
    return localAckerBuilder.props(ackers, nodeId, defaultMinimalTime);
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
    private int partitions = Runtime.getRuntime().availableProcessors();

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

    public Builder partitions(int partitions) {
      this.partitions = partitions;
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
              workersResourcesDistributor,
              partitions
      );
    }
  }
}
