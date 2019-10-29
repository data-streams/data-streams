package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class MapJoba extends Joba {
  private final FlameMap<?, ?>.FlameMapOperation operation;

  public MapJoba(Joba.Id id, FlameMap<?, ?> flameMap) {
    super(id);
    this.operation = flameMap.operation(ThreadLocalRandom.current().nextLong());
  }

  @Override
  public void accept(
          DataItem item,
          Consumer<DataItem> sink,
          int vertexIndex,
          Consumer<Supplier<Stream<DataItem>>> scheduleDoneSnapshot
  ) {
    operation.apply(item, vertexIndex, scheduleDoneSnapshot).forEach(sink);
  }
}
