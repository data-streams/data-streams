package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.LabelSpawn;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;

public class LabelSpawnJoba extends Joba {
  private final Function<DataItem, DataItem> operation;

  public LabelSpawnJoba(Id id, LabelSpawn<?, ?> flameMap) {
    super(id);
    this.operation = flameMap.operation(ThreadLocalRandom.current().nextLong(), id.nodeId);
  }

  @Override
  public void accept(DataItem item, Consumer<DataItem> sink) {
    sink.accept(operation.apply(item));
  }
}
