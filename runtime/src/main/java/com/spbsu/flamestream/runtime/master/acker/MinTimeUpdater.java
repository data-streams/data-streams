package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.MinTimeUpdateListener;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class MinTimeUpdater {
  private class ShardState {
    GlobalTime minTime = lastMinTime.minTime();
    NodeTimes operationsTime = new NodeTimes();
    TreeMap<GlobalTime, NodeTimes> minTimeUpdates = new TreeMap<>();
  }

  private Map<ActorRef, ShardState> shardStates;

  private MinTimeUpdate lastMinTime;

  public MinTimeUpdater(List<ActorRef> shards, long defaultMinimalTime) {
    lastMinTime = new MinTimeUpdate(new GlobalTime(defaultMinimalTime, EdgeId.Min.INSTANCE), new NodeTimes());
    this.shardStates = shards.stream().collect(Collectors.toMap(Function.identity(), __ -> new ShardState()));
  }

  public void subscribe(ActorRef self) {
    shardStates.keySet().forEach(acker -> acker.tell(new MinTimeUpdateListener(self), self));
  }

  @Nullable
  public MinTimeUpdate onShardMinTimeUpdate(ActorRef shard, MinTimeUpdate shardMinTimeUpdate) {
    ShardState shardState = shardStates.get(shard);
    shardState.operationsTime = shardMinTimeUpdate.getNodeTimes();
    shardState.minTimeUpdates.put(shardMinTimeUpdate.minTime(), shardMinTimeUpdate.getNodeTimes());
    final MinTimeUpdate minAmongTables = minAmongTables();
    if (minAmongTables.minTime().compareTo(lastMinTime.minTime()) > 0) {
      this.lastMinTime = minAmongTables;
      return minAmongTables;
    }
    return null;
  }

  private MinTimeUpdate minAmongTables() {
    if (shardStates.isEmpty()) {
      return lastMinTime;
    }
    NodeTimes min =
            shardStates.values()
                    .stream()
                    .map(shardState -> shardState.operationsTime)
                    .reduce(new NodeTimes(), NodeTimes::min);
    for (ShardState shardState : shardStates.values()) {
      final Map.Entry<GlobalTime, NodeTimes> minTimeUpdate = shardState.minTimeUpdates.entrySet()
              .stream()
              .filter(entry -> entry.getValue().greaterThanOrNotComparableTo(min))
              .findFirst()
              .map(entry -> shardState.minTimeUpdates.lowerEntry(entry.getKey()))
              .orElse(shardState.minTimeUpdates.lastEntry());
      if (minTimeUpdate == null) {
        continue;
      }
      shardState.minTime = minTimeUpdate.getKey();
      shardState.minTimeUpdates.tailMap(shardState.minTime).clear();
    }
    return new MinTimeUpdate(Collections.min(shardStates.values()
            .stream()
            .map(shardState -> shardState.minTime)
            .collect(Collectors.toList())), min);
  }
}
