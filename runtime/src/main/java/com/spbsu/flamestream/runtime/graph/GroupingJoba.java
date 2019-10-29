package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.runtime.graph.state.GroupGroupingState;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class GroupingJoba extends Joba {
  private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("grouping-receive");

  private final Grouping<?> grouping;
  private final GroupGroupingState state;
  private final Grouping<?>.GroupingOperation instance;

  private GlobalTime currentMinTime = GlobalTime.MIN;

  public GroupingJoba(Joba.Id id, Grouping<?> grouping, GroupGroupingState state) {
    super(id);
    this.instance = grouping.operation(ThreadLocalRandom.current().nextLong());
    this.grouping = grouping;
    this.state = state;
  }

  @Override
  public void accept(DataItem item,
                     Consumer<DataItem> sink,
                     int vertexIndex,
                     Consumer<Supplier<Stream<DataItem>>> supplierConsumer) {
    tracer.log(item.xor());

    final InvalidatingBucket bucket = state.bucketFor(item, grouping.hash(), grouping.equalz());
    instance.apply(item, bucket).forEach(sink);
    { //clear outdated
      final int position = Math.max(bucket.lowerBound(new Meta(currentMinTime)) - grouping.window() + 1, 0);
      bucket.clearRange(0, position);
    }
  }

  @Override
  public void onPrepareCommit(GlobalTime time) {
    currentMinTime = time;
  }
}
