package com.spbsu.flamestream.runtime.graph.state;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.HashUnit;
import com.spbsu.flamestream.runtime.utils.collections.HashUnitMap;
import com.spbsu.flamestream.runtime.utils.collections.ListHashUnitMap;

import java.util.Collection;

public class GroupGroupingState {
  private final Grouping<?> grouping;
  private final HashUnitMap<GroupingState> unitStates = new ListHashUnitMap<>();

  public GroupGroupingState(Grouping<?> grouping) {
    this.grouping = grouping;
  }

  public GroupGroupingState(Grouping<?> grouping, Collection<HashUnit> units) {
    this.grouping = grouping;
    units.forEach(u -> unitStates.put(u, new GroupingState(grouping)));
  }

  public void addUnitState(HashUnit unit, GroupingState unitState) {
    unitStates.put(unit, unitState);
  }

  public InvalidatingBucket bucketFor(DataItem item) {
    final GroupingState groupingState;
    if (grouping.hash() == HashFunction.PostBroadcast.INSTANCE) {
      // hack for broadcast
      groupingState = unitStates.first();
    } else {
      groupingState = unitStates.get(grouping.hash().hash(item));
    }
    return groupingState.bucketFor(item);
  }

  public void onMinTime(long time) {
    unitStates.entrySet().forEach(entry -> entry.getValue().onMinTime(time));
  }
}
