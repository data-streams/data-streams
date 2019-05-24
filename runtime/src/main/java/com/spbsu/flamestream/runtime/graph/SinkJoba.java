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
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.edge.api.GimmeLastBatch;
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
  private final InvalidatingBucket invalidatingBucket = new ArrayInvalidatingBucket();
  private final Map<ActorRef, GlobalTime> rears = new HashMap<>();

  private final Tracing.Tracer barrierReceiveTracer = Tracing.TRACING.forEvent("barrier-receive");
  private final Tracing.Tracer barrierSendTracer = Tracing.TRACING.forEvent("barrier-send");

  private GlobalTime minTime = GlobalTime.MIN;

  SinkJoba(Joba.Id id, ActorContext context, boolean barrierDisabled) {
    super(id);
    log = Logging.getLogger(context.system(), context.self());
    this.barrierDisabled = barrierDisabled;
  }

  @Override
  public void accept(DataItem item, Consumer<DataItem> sink) {
    barrierReceiveTracer.log(item.xor());
    if (barrierDisabled) {
      rears.forEach((rear, lastEmmit) -> emmitRearBatch(
              rear,
              new BatchImpl(item.meta().globalTime(), Collections.singletonList(item))
      ));
    } else {
      invalidatingBucket.insert(item);
    }
  }

  public void attachRear(ActorRef rear) {
    try {
      final Batch batch = PatternsCS.ask(rear, new GimmeLastBatch(), FlameConfig.config.bigTimeout())
              .thenApply(e -> (Batch) e)
              .toCompletableFuture().get();
      if (batch == Batch.Default.EMPTY) {
        rears.put(rear, GlobalTime.MIN);
      } else {
        final GlobalTime time = ((BatchImpl) batch).time();
        rears.put(rear, time);
      }

      tryEmmit(minTime);
      log.info("Attached rear to graph {}", rears.get(rear));
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    this.minTime = minTime;
    tryEmmit(minTime);
  }

  private void tryEmmit(GlobalTime upTo) {
    final int pos = invalidatingBucket.lowerBound(new Meta(upTo));

    rears.forEach((rear, lastEmmit) -> {
      final List<DataItem> data = new ArrayList<>();
      invalidatingBucket.forRange(0, pos, item -> {
        if (item.meta().globalTime().compareTo(lastEmmit) > 0) {
          data.add(item);
        }
      });

      if (!data.isEmpty()) {
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

    public GlobalTime time() {
      return time;
    }

    @Override
    public <T> Stream<T> payload(Class<T> clazz) {
      return items.stream().map(i -> i.payload(clazz));
    }
  }
}
