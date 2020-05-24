package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.LabelSpawn;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class LabelSpawnJoba extends Joba {
  private final LabelSpawn<?, ?>.LabelsInUse operation;
  private final int sinkTrackingComponent;

  public LabelSpawnJoba(
          Id id,
          LabelSpawn<?, ?> flameMap,
          Iterable<Sink> markers,
          int sinkTrackingComponent
  ) {
    super(id);
    this.sinkTrackingComponent = sinkTrackingComponent;
    this.operation = flameMap.operation(ThreadLocalRandom.current().nextLong(), id.nodeId, markers);
  }

  @Override
  public void accept(DataItem item, Sink sink) {
    sink.accept(operation.apply(item));
  }

  @Override
  void onMinTime(MinTimeUpdate time) {
    if (time.trackingComponent() == sinkTrackingComponent)
      operation.onMinTime(time.minTime().time());
  }
}
