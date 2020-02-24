package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.LabelMarkers;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.function.Consumer;

public class LabelMarkersJoba extends Joba {
  private interface InboundMinTime {
    GlobalTime accept(GlobalTime globalTime);

    class Impl implements InboundMinTime {
      private final int[] sortedTrackingComponent;
      private final GlobalTime[] inboundMinTime;
      private GlobalTime minMinTime;

      public Impl(TrackingComponent trackingComponent) {
        this.sortedTrackingComponent = new int[trackingComponent.inbound.size()];
        int i = 0;
        for (final TrackingComponent component : trackingComponent.inbound) {
          sortedTrackingComponent[i] = component.index;
          i++;
        }
        Arrays.sort(sortedTrackingComponent);
        this.inboundMinTime = new GlobalTime[trackingComponent.inbound.size()];
      }

      @Override
      public GlobalTime accept(GlobalTime globalTime) {
        final int i = Arrays.binarySearch(sortedTrackingComponent, globalTime.trackingComponent());
        if (i < 0 || sortedTrackingComponent[i] != globalTime.trackingComponent()) {
          return null;
        }
        final int index = sortedTrackingComponent[i];
        if (globalTime.equals(inboundMinTime[index])) {
          return null;
        }
        if (inboundMinTime[index] != null && globalTime.compareTo(inboundMinTime[index]) < 0) {
          throw new IllegalArgumentException();
        }
        inboundMinTime[index] = globalTime;
        for (final GlobalTime time : inboundMinTime) {
          if (time == null)
            return null;
          if (time.compareTo(globalTime) < 0) {
            globalTime = time;
          }
        }
        if (globalTime.equals(minMinTime)) {
          return null;
        }
        minMinTime = globalTime;
        return globalTime;
      }
    }
  }

  @org.jetbrains.annotations.NotNull
  private final LabelMarkers<?> labelMarkers;
  private final Consumer<DataItem> sink;
  private final InboundMinTime inboundMinTime;
  private final PriorityQueue<DataItem> scheduled =
          new PriorityQueue<>(Comparator.comparing(dataItem -> dataItem.meta().globalTime()));

  public LabelMarkersJoba(Id id, LabelMarkers<?> labelMarkers, Consumer<DataItem> sink) {
    super(id);
    this.labelMarkers = labelMarkers;
    this.sink = sink;
    inboundMinTime = new InboundMinTime.Impl(labelMarkers.trackingComponent);
  }

  @Override
  void onMinTime(GlobalTime time) {
    if ((time = inboundMinTime.accept(time)) != null) {
      while (!scheduled.isEmpty()) {
        final DataItem dataItem = scheduled.peek();
        if (time.compareTo(dataItem.meta().globalTime()) < 0)
          break;
        sink.accept(dataItem);
        scheduled.poll();
      }
    }
  }

  @Override
  public void accept(DataItem item, Consumer<DataItem> sink) {
    sink.accept(item);
    //scheduled.add(item);
  }
}
