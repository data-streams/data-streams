package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Label;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.List;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class LabelSpawn<T, L> extends Graph.Vertex.Stub {
  private final Class<T> tClass;
  private final int index;
  private final SerializableFunction<T, L> mapper;
  private final List<? extends LabelMarkers> labelMarkers;

  public LabelSpawn(
          Class<T> tClass,
          int index,
          SerializableFunction<T, L> mapper,
          List<? extends LabelMarkers> labelMarkers
  ) {
    this.tClass = tClass;
    this.index = index;
    this.mapper = mapper;
    this.labelMarkers = labelMarkers;
  }

  public Stream<? extends LabelMarkers> labelMarkers() {
    return labelMarkers.stream();
  }

  public class LabelsInUse implements Function<DataItem, DataItem> {
    private final long physicalId;
    private final String nodeId;
    private final Iterable<Consumer<DataItem>> markers;
    private final TreeMap<Long, Integer> timeSequences = new TreeMap<>();

    private LabelsInUse(long physicalId, String nodeId, Iterable<Consumer<DataItem>> markers) {
      this.physicalId = physicalId;
      this.nodeId = nodeId;
      this.markers = markers;
    }

    @Override
    public DataItem apply(DataItem in) {
      final T payload = in.payload(tClass);
      final L value = mapper.apply(payload);
      final long time = in.meta().globalTime().time();
      final Label label = new Label(index, nodeId, time, timeSequences.merge(time, 1, Integer::sum) - 1);
      int childId = 0;
      final PayloadDataItem dataItem = new PayloadDataItem(
              new Meta(in.meta(), physicalId, childId++),
              payload,
              in.labels().added(label)
      );
      for (final Consumer<DataItem> marker : markers) {
        marker.accept(new PayloadDataItem(
                new Meta(in.meta(), physicalId, childId++),
                value,
                in.labels().added(label)
        ));
      }
      return dataItem;
    }

    public void onMinTime(long time) {
      timeSequences.headMap(time).clear();
    }

  }

  public LabelsInUse operation(long physicalId, String nodeId, Iterable<Consumer<DataItem>> markers) {
    return new LabelsInUse(physicalId, nodeId, markers);
  }
}
