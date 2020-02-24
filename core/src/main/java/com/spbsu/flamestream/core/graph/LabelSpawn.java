package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Label;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class LabelSpawn<T, L> extends Graph.Vertex.Stub {
  private final Class<T> tClass;
  private final int index;
  private final FlameMap.SerializableFunction<T, L> mapper;
  private final List<? extends LabelMarkers<?>> labelMarkers;

  public LabelSpawn(
          Class<T> tClass,
          int index,
          FlameMap.SerializableFunction<T, L> mapper,
          List<? extends LabelMarkers<?>> labelMarkers
  ) {
    this.tClass = tClass;
    this.index = index;
    this.mapper = mapper;
    this.labelMarkers = labelMarkers;
  }

  public Stream<? extends LabelMarkers<?>> labelMarkers() {
    return labelMarkers.stream();
  }

  private class LabelsInUse {
    final String nodeId;
    final Map<L, BitSet> valueUniqueness = new HashMap<>();

    private LabelsInUse(String nodeId) {this.nodeId = nodeId;}

    Label<L> lock(T payload) {
      final L label = mapper.apply(payload);
      final BitSet locked = valueUniqueness.computeIfAbsent(label, __ -> new BitSet());
      final int uniqueness = locked.nextClearBit(0);
      locked.set(uniqueness);
      return new Label<>(index, label, nodeId, uniqueness);
    }

    void unlock(Label<L> label) {
      valueUniqueness.get(label.value).clear(label.uniqueness);
    }
  }

  public Function<DataItem, DataItem> operation(long physicalId, String nodeId, Iterable<Consumer<DataItem>> markers) {
    final LabelsInUse labelsInUse = new LabelsInUse(nodeId);
    return in -> {
      final T payload = in.payload(tClass);
      int childId = 0;
      final Label<L> label = labelsInUse.lock(payload);
      final PayloadDataItem dataItem = new PayloadDataItem(
              new Meta(in.meta(), physicalId, childId++),
              payload,
              in.labels().added(label)
      );
      for (final Consumer<DataItem> marker : markers) {
        marker.accept(new PayloadDataItem(
                new Meta(in.meta(), physicalId, childId++),
                label.value,
                in.labels().added(label)
        ));
      }
      return dataItem;
    };
  }
}
