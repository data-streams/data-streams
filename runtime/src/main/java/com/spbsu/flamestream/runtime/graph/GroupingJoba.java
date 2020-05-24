package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.runtime.graph.state.GroupGroupingState;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class GroupingJoba extends Joba {
  private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("grouping-receive");

  private final Grouping<?> grouping;
  private final GroupGroupingState state;
  private final int sinkTrackingComponent;
  private final Grouping<?>.GroupingOperation instance;

  private GlobalTime currentMinTime = GlobalTime.MIN;

  public GroupingJoba(Id id, Grouping<?> grouping, GroupGroupingState state, int sinkTrackingComponent) {
    super(id);
    this.instance = grouping.operation(ThreadLocalRandom.current().nextLong());
    this.grouping = grouping;
    this.state = state;
    this.sinkTrackingComponent = sinkTrackingComponent;
  }

  @Override
  public boolean accept(DataItem item, Sink sink) {
    tracer.log(item.xor());

    final InvalidatingBucket bucket = state.bucketFor(item);
    instance.apply(item, bucket).forEach(sink);
    { //clear outdated
      final int position = Math.max(bucket.lowerBound(currentMinTime) - grouping.window() + 1, 0);
      bucket.clearRange(0, position);
    }
    return true;
  }

  @Override
  List<DataItem> onMinTime(MinTimeUpdate time) {
    if (time.trackingComponent() == sinkTrackingComponent)
      state.onMinTime(time.minTime().time());
    return super.onMinTime(time);
  }

  @Override
  public void onPrepareCommit(GlobalTime time) {
    currentMinTime = time;
  }
}
