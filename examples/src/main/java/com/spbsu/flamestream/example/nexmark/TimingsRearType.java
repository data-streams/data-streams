package com.spbsu.flamestream.example.nexmark;

import com.spbsu.flamestream.core.Batch;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.graph.SinkJoba;
import org.apache.commons.lang.ArrayUtils;

import java.lang.reflect.InvocationTargetException;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class TimingsRearType<Wrapped extends com.spbsu.flamestream.runtime.edge.Rear, Handle>
        implements FlameRuntime.RearType<TimingsRearType.Rear, Handle> {
  public static class Timings {
    public final long window;
    public final String nodeId;
    public final long processed;
    public final long notified;

    public Timings(long window, String nodeId, long processed, long notified) {
      this.window = window;
      this.nodeId = nodeId;
      this.processed = processed;
      this.notified = notified;
    }

    @Override
    public String toString() {
      return withDigitSeparators(window) + " seconds window timings " + nodeId
              + ": processed = " + withDigitSeparators(processed)
              + ", notified = " + withDigitSeparators(notified)
              ;
    }

    private static String withDigitSeparators(long nanos) {
      return NumberFormat.getNumberInstance(Locale.US).format(nanos);
    }
  }

  final FlameRuntime.RearType<Wrapped, Handle> wrapped;
  final long windowSizeSec;
  private final Instant baseTime;

  public TimingsRearType(FlameRuntime.RearType<Wrapped, Handle> wrapped, long windowSizeSec, Instant baseTime) {
    this.wrapped = wrapped;
    this.windowSizeSec = windowSizeSec;
    this.baseTime = baseTime;
  }

  public class Instance implements FlameRuntime.RearInstance<TimingsRearType.Rear> {
    private final FlameRuntime.RearInstance<Wrapped> wrapped;
    private final long windowSizeSec = TimingsRearType.this.windowSizeSec;

    public Instance(FlameRuntime.RearInstance<Wrapped> wrapped) {this.wrapped = wrapped;}

    @Override
    public Class<TimingsRearType.Rear> clazz() {
      return TimingsRearType.Rear.class;
    }

    @Override
    public Object[] params() {
      return new Object[]{this, wrapped.params()};
    }

    public TimingsRearType<?, ?> type() {
      return TimingsRearType.this;
    }
  }

  public static class Rear implements com.spbsu.flamestream.runtime.edge.Rear {
    private final EdgeContext edgeContext;
    @org.jetbrains.annotations.NotNull
    private final TimingsRearType<?, ?>.Instance instance;
    private final com.spbsu.flamestream.runtime.edge.Rear wrapped;

    public Rear(
            EdgeContext edgeContext, TimingsRearType<?, ?>.Instance instance, Object[] params
    ) throws IllegalAccessException, InvocationTargetException, InstantiationException {
      this.edgeContext = edgeContext;
      this.instance = instance;
      wrapped = (com.spbsu.flamestream.runtime.edge.Rear) instance.wrapped.clazz().getDeclaredConstructors()[0]
              .newInstance(ArrayUtils.add(params, 0, edgeContext));
    }

    Batch last = Batch.Default.EMPTY;

    @Override
    public CompletionStage<?> accept(Batch batch) {
      if (batch.time().time() < Long.MAX_VALUE) {
        final var window = batch.time().time() - instance.windowSizeSec;
        final var processed = batch.lastGlobalTimeProcessedAt().get(window);
        final var now = Instant.now();
        final var windowEnd = Instant.ofEpochSecond(
                Query8.tumbleStart(instance.type().baseTime.plus(window, ChronoUnit.SECONDS), instance.windowSizeSec)
        );
        return wrapped.accept(new SinkJoba.BatchImpl(
                batch.time(),
                Collections.singletonList(new PayloadDataItem(new Meta(batch.time()), new Timings(
                        window,
                        edgeContext.edgeId().nodeId(),
                        windowEnd.until(processed, ChronoUnit.NANOS),
                        windowEnd.until(now, ChronoUnit.NANOS)
                ))),
                batch.lastGlobalTimeProcessedAt()
        ));
      }
      last = batch;
      return CompletableFuture.completedStage(null);
    }

    @Override
    public Batch last() {
      return last;
    }
  }

  @Override
  public Instance instance() {
    return new Instance(wrapped.instance());
  }

  @Override
  public Handle handle(EdgeContext context) {
    return wrapped.handle(context);
  }
}
