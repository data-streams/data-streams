package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Label;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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

  public class LabelsInUse implements Function<DataItem, DataItem> {
    private final long physicalId;
    private final String nodeId;
    private final Iterable<Consumer<DataItem>> markers;
    private final Map<L, BitSet> valueUniqueness = new HashMap<>();
    private final TreeMap<Long, List<Label<L>>> timeLabels = new TreeMap<>();

    private LabelsInUse(long physicalId, String nodeId, Iterable<Consumer<DataItem>> markers) {
      this.physicalId = physicalId;
      this.nodeId = nodeId;
      this.markers = markers;
    }

    @Override
    public DataItem apply(DataItem in) {
      final T payload = in.payload(tClass);
      int childId = 0;
      final Label<L> label = lock(payload);
      timeLabels.computeIfAbsent(in.meta().globalTime().time(), __ -> new ArrayList<>()).add(label);
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
    }

    public void onMinTime(long time) {
      while (!timeLabels.isEmpty()) {
        if (time < timeLabels.firstKey())
          break;
        timeLabels.pollFirstEntry().getValue().forEach(this::unlock);
      }
    }

    private Label<L> lock(T payload) {
      final L label = mapper.apply(payload);
      final BitSet locked = valueUniqueness.computeIfAbsent(label, __ -> new BitSet());
      final int uniqueness = locked.nextClearBit(0);
      locked.set(uniqueness);
      return new Label<>(index, label, nodeId, uniqueness);
    }

    private void unlock(Label<L> label) {
      valueUniqueness.get(label.value).clear(label.uniqueness);
    }
  }

  public LabelsInUse operation(long physicalId, String nodeId, Iterable<Consumer<DataItem>> markers) {
    return new LabelsInUse(physicalId, nodeId, markers);
  }
}
