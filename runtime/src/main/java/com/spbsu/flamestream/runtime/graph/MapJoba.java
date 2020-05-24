package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.concurrent.ThreadLocalRandom;

public class MapJoba extends Joba {
  private final FlameMap<?, ?>.FlameMapOperation operation;

  public MapJoba(Joba.Id id, FlameMap<?, ?> flameMap) {
    super(id);
    this.operation = flameMap.operation(ThreadLocalRandom.current().nextLong());
  }

  @Override
  public void accept(DataItem item, Sink sink) {
    operation.apply(item).forEach(sink);
  }
}
