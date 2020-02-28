package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.LabelMarkers;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Consumer;

public class LabelMarkersJoba extends Joba {
  private interface InboundMinTime {
    long accept(MinTimeUpdate globalTime);

    long get();

    class Impl implements InboundMinTime {
      private final int[] sortedTrackingComponent;
      private final long[] inboundMinTime;
      private long minMinTime;

      public Impl(TrackingComponent trackingComponent) {
        this.sortedTrackingComponent = new int[trackingComponent.inbound.size()];
        int i = 0;
        for (final TrackingComponent component : trackingComponent.inbound) {
          sortedTrackingComponent[i] = component.index;
          i++;
        }
        Arrays.sort(sortedTrackingComponent);
        this.inboundMinTime = new long[trackingComponent.inbound.size()];
        Arrays.fill(inboundMinTime, minMinTime = Long.MIN_VALUE);
      }

      @Override
      public long accept(MinTimeUpdate globalTime) {
        final int i = Arrays.binarySearch(sortedTrackingComponent, globalTime.trackingComponent());
        if (i < 0 || sortedTrackingComponent[i] != globalTime.trackingComponent()) {
          return Long.MIN_VALUE;
        }
        final int index = sortedTrackingComponent[i];
        if (globalTime.minTime().time() <= inboundMinTime[index]) {
          throw new IllegalArgumentException();
        }
        inboundMinTime[index] = globalTime.minTime().time();
        return minMinTime = Arrays.stream(inboundMinTime).min().getAsLong();
      }

      @Override
      public long get() {
        return minMinTime;
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
  List<DataItem> onMinTime(MinTimeUpdate componentTime) {
    final List<DataItem> accepted = new ArrayList<>();
    final long inboundTime = inboundMinTime.accept(componentTime);
    while (!scheduled.isEmpty()) {
      final DataItem dataItem = scheduled.peek();
      if (inboundTime <= dataItem.meta().globalTime().time()) {
        break;
      }
      accepted.add(dataItem);
      sink.accept(dataItem.cloneWith(dataItem.meta()));
      scheduled.poll();
    }
    return accepted;
  }

  @Override
  public boolean accept(DataItem item, Consumer<DataItem> sink) {
    if (inboundMinTime.get() <= item.meta().globalTime().time()) {
      scheduled.add(item);
      return false;
    } else {
      sink.accept(item.cloneWith(item.meta()));
      return true;
    }
  }
}
