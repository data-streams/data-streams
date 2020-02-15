package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Label;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class LabelSpawn<T, L> extends Graph.Vertex.Stub {
  private final Class<T> tClass;
  private final int index;
  private final FlameMap.SerializableFunction<T, L> mapper;

  public LabelSpawn(Class<T> tClass, int index, FlameMap.SerializableFunction<T, L> mapper) {
    this.tClass = tClass;
    this.index = index;
    this.mapper = mapper;
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

  public Function<DataItem, DataItem> operation(long physicalId, String nodeId) {
    final LabelsInUse labelsInUse = new LabelsInUse(nodeId);
    return in -> {
      final T payload = in.payload(tClass);
      return new PayloadDataItem(
              new Meta(in.meta(), physicalId, 0),
              payload,
              in.labels().added(labelsInUse.lock(payload))
      );
    };
  }
}
