package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.edge.api.Checkpoint;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class SourceJoba extends Joba {
  private final Collection<GlobalTime> inFlight = new ArrayList<>();
  private final Map<EdgeId, ActorRef> fronts = new HashMap<>();
  private final int maxInFlightItems;
  private final ActorContext context;

  private int unutilizedRequests;
  private final boolean barrierIsDisabled;
  private final int sinkTrackingComponent;

  public SourceJoba(
          Id id,
          int maxInFlightItems,
          ActorContext context,
          boolean barrierIsDisabled,
          int sinkTrackingComponent
  ) {
    super(id);
    this.maxInFlightItems = maxInFlightItems;
    this.context = context;
    this.unutilizedRequests = maxInFlightItems;
    this.barrierIsDisabled = barrierIsDisabled;
    this.sinkTrackingComponent = sinkTrackingComponent;
  }

  @Override
  public boolean accept(DataItem item, Consumer<DataItem> sink) {
    sink.accept(item);
    unutilizedRequests--;
    final GlobalTime globalTime = item.meta().globalTime();
    if (!barrierIsDisabled) {
      inFlight.add(globalTime);
    }
    requestNext();
    return true;
  }

  @Override
  public List<DataItem> onMinTime(MinTimeUpdate minTime) {
    if (minTime.trackingComponent() == sinkTrackingComponent) {
      inFlight.removeIf(nextTime -> nextTime.compareTo(minTime.minTime()) < 0);
      requestNext();
    }
    return Collections.emptyList();
  }

  public void addFront(EdgeId front, ActorRef actorRef) {
    fronts.putIfAbsent(front, actorRef);
  }

  public void checkpoint(GlobalTime globalTime) {
    fronts.values().forEach(actorRef -> actorRef.tell(new Checkpoint(globalTime), context.self()));
  }

  private void requestNext() {
    while (inFlight.size() + unutilizedRequests < maxInFlightItems) {
      unutilizedRequests++;
      fronts.values().forEach(f -> f.tell(new RequestNext(), context.self()));
    }
  }
}
