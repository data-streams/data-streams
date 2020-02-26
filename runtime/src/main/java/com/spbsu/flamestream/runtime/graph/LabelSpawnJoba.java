package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.LabelSpawn;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class LabelSpawnJoba extends Joba {
  private final LabelSpawn<?, ?>.LabelsInUse operation;
  private final int sinkTrackingComponent;

  public LabelSpawnJoba(
          Id id,
          LabelSpawn<?, ?> flameMap,
          Iterable<Consumer<DataItem>> markers,
          int sinkTrackingComponent
  ) {
    super(id);
    this.sinkTrackingComponent = sinkTrackingComponent;
    this.operation = flameMap.operation(ThreadLocalRandom.current().nextLong(), id.nodeId, markers);
  }

  @Override
  public boolean accept(DataItem item, Consumer<DataItem> sink) {
    sink.accept(operation.apply(item));
    return true;
  }

  @Override
  List<DataItem> onMinTime(MinTimeUpdate time) {
    if (time.trackingComponent() == sinkTrackingComponent)
      operation.onMinTime(time.minTime().time());
    return Collections.emptyList();
  }
}
