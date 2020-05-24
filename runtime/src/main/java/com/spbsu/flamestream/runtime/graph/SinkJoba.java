package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.PatternsCS;
import com.spbsu.flamestream.core.Batch;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.edge.api.GimmeLastBatch;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SinkJoba extends Joba {
  private final LoggingAdapter log;
  private final boolean barrierDisabled;
  private final int sinkTrackingComponent;
  private final InvalidatingBucket invalidatingBucket = new ArrayInvalidatingBucket();
  private final Map<ActorRef, GlobalTime> rears = new HashMap<>();

  private final Tracing.Tracer barrierReceiveTracer = Tracing.TRACING.forEvent("barrier-receive");
  private final Tracing.Tracer barrierSendTracer = Tracing.TRACING.forEvent("barrier-send");

  private GlobalTime minTime = GlobalTime.MIN;

  SinkJoba(Id id, ActorContext context, boolean barrierDisabled, int sinkTrackingComponent) {
    super(id);
    log = Logging.getLogger(context.system(), context.self());
    this.barrierDisabled = barrierDisabled;
    this.sinkTrackingComponent = sinkTrackingComponent;
  }

  @Override
  public boolean accept(DataItem item, Sink sink) {
    barrierReceiveTracer.log(item.xor());
    if (barrierDisabled) {
      rears.forEach((rear, lastEmmit) -> emmitRearBatch(
              rear,
              new BatchImpl(item.meta().globalTime(), Collections.singletonList(item))
      ));
    } else {
      invalidatingBucket.insert(item);
    }
    return true;
  }

  public void attachRear(ActorRef rear) {
    try {
      final Batch batch = PatternsCS.ask(rear, new GimmeLastBatch(), FlameConfig.config.bigTimeout())
              .thenApply(e -> (Batch) e)
              .toCompletableFuture().get();
      rears.put(rear, batch.time());

      tryEmmit(minTime);
      log.info("Attached rear to graph {}", rears.get(rear));
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<DataItem> onMinTime(MinTimeUpdate minTime) {
    if (minTime.trackingComponent() == sinkTrackingComponent) {
      this.minTime = minTime.minTime();
      tryEmmit(minTime.minTime());
    }
    return Collections.emptyList();
  }

  private void tryEmmit(GlobalTime upTo) {
    final int pos = invalidatingBucket.lowerBound(upTo);

    rears.forEach((rear, lastEmmit) -> {
      final List<DataItem> data = new ArrayList<>();
      invalidatingBucket.forRange(0, pos, item -> {
        if (item.meta().globalTime().compareTo(lastEmmit) > 0) {
          data.add(item);
        }
      });

      if (!data.isEmpty() || barrierDisabled) {
        emmitRearBatch(rear, new BatchImpl(upTo, data));
      }
    });

    // Clearing barrier only if elements were emitted somewhere.
    // It is temporary fix of the "sending elements to /dev/null" problem
    //
    // https://github.com/flame-stream/FlameStream/issues/139
    if (!rears.isEmpty()) {
      invalidatingBucket.clearRange(0, pos);
    }
  }

  private void emmitRearBatch(ActorRef rear, BatchImpl batch) {
    try {
      PatternsCS.ask(rear, batch, FlameConfig.config.smallTimeout()).toCompletableFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    barrierSendTracer.log(batch.time().time());
  }

  public static class BatchImpl implements Batch {
    private final List<DataItem> items;
    private final GlobalTime time;

    private BatchImpl(GlobalTime time, List<DataItem> items) {
      this.items = items;
      this.time = time;
    }

    @Override
    public GlobalTime time() {
      return time;
    }

    @Override
    public Stream<DataItem> payload() {
      return items.stream();
    }
  }
}
