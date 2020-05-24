package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.LabelSpawn;

import java.util.concurrent.ThreadLocalRandom;

public class LabelSpawnJoba extends Joba {
  private final LabelSpawn<?, ?>.LabelsInUse operation;

  public LabelSpawnJoba(Id id, LabelSpawn<?, ?> flameMap, Iterable<Sink> markers) {
    super(id);
    this.operation = flameMap.operation(ThreadLocalRandom.current().nextLong(), markers);
  }

  @Override
  public void accept(DataItem item, Sink sink) {
    sink.accept(operation.apply(item));
  }
}
