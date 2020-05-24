package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.LabelMarkers;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import org.jetbrains.annotations.NotNull;
import scala.util.Left;
import scala.util.Right;

import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;

public class LabelMarkersJoba extends Joba {
  private final long physicalId = ThreadLocalRandom.current().nextLong();
  @org.jetbrains.annotations.NotNull
  private final LabelMarkers labelMarkers;
  private long inboundMinTime;

  private static final class Scheduled implements Comparable<Scheduled> {
    static final Comparator<Scheduled> COMPARATOR =
            Comparator.comparing(scheduled -> scheduled.dataItem.meta().globalTime());

    final DataItem dataItem;
    final Runnable send;

    private Scheduled(DataItem dataItem, Runnable send) {
      this.dataItem = dataItem;
      this.send = send;
    }

    @Override
    public int compareTo(@NotNull Scheduled o) {
      return COMPARATOR.compare(this, o);
    }
  }

  private final PriorityQueue<Scheduled> scheduled = new PriorityQueue<>();

  public LabelMarkersJoba(Id id, LabelMarkers labelMarkers) {
    super(id);
    this.labelMarkers = labelMarkers;
    inboundMinTime = Long.MIN_VALUE;
  }

  @Override
  void onMinTime(MinTimeUpdate componentTime) {
    if (componentTime.trackingComponent() != labelMarkers.trackingComponent.index) {
      return;
    }
    inboundMinTime = componentTime.minTime().time();
    while (!scheduled.isEmpty()) {
      final Scheduled scheduled = this.scheduled.peek();
      if (inboundMinTime <= scheduled.dataItem.meta().globalTime().time()) {
        break;
      }
      scheduled.send.run();
      this.scheduled.poll();
    }
  }

  @Override
  public void accept(DataItem item, Sink sink) {
    final Meta meta = new Meta(item.meta(), physicalId, 0);
    if (item.marker()) {
      final PayloadDataItem marker = new PayloadDataItem(meta, new Right<>(item.payload(Object.class)), item.labels());
      scheduled.add(new Scheduled(marker, sink.schedule(marker)));
    } else {
      sink.accept(new PayloadDataItem(meta, new Left<>(item.payload(Object.class)), item.labels()));
    }
  }
}
