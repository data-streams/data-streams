package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Label;
import com.spbsu.flamestream.core.data.meta.Labels;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.List;
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
    private final Iterable<? extends Consumer<DataItem>> markers;

    private LabelsInUse(long physicalId, Iterable<? extends Consumer<DataItem>> markers) {
      this.physicalId = physicalId;
      this.markers = markers;
    }

    @Override
    public DataItem apply(DataItem in) {
      final T payload = in.payload(tClass);
      final L value = mapper.apply(payload);
      final Label label = new Label(index, in.meta());
      int childId = 0;
      final Labels newLabels = in.meta().labels().added(label);
      final PayloadDataItem dataItem = new PayloadDataItem(
              new Meta(in.meta(), physicalId, childId++, newLabels),
              payload
      );
      for (final Consumer<DataItem> marker : markers) {
        marker.accept(new PayloadDataItem(new Meta(in.meta(), physicalId, childId++, newLabels), value, true));
      }
      return dataItem;
    }
  }

  public LabelsInUse operation(long physicalId, Iterable<? extends Consumer<DataItem>> markers) {
    return new LabelsInUse(physicalId, markers);
  }
}
