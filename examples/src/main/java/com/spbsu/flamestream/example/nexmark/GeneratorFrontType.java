package com.spbsu.flamestream.example.nexmark;

import com.github.nexmark.flink.NexmarkConfiguration;
import com.github.nexmark.flink.generator.GeneratorConfig;
import com.github.nexmark.flink.generator.NexmarkGenerator;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class GeneratorFrontType implements FlameRuntime.FrontType<GeneratorFrontType.Front, Void> {
  private final NexmarkConfiguration nexmarkConfiguration;
  private final Map<String, Integer> nodePartition;
  private final long baseTime;
  private final long maxEvents;

  public GeneratorFrontType(
          NexmarkConfiguration nexmarkConfiguration,
          Map<String, Integer> nodePartition,
          long baseTime,
          long maxEvents
  ) {
    this.nexmarkConfiguration = nexmarkConfiguration;
    this.nodePartition = nodePartition;
    this.baseTime = baseTime;
    this.maxEvents = maxEvents;
  }

  public class Instance implements FlameRuntime.FrontInstance<Front> {
    @Override
    public Class<Front> clazz() {
      return Front.class;
    }

    @Override
    public Object[] params() {
      return new Object[]{GeneratorFrontType.this};
    }
  }

  public static class Front implements com.spbsu.flamestream.runtime.edge.Front {
    private final EdgeContext edgeContext;
    @org.jetbrains.annotations.NotNull
    private final GeneratorFrontType type;
    private final Integer partition;

    public Front(EdgeContext edgeContext, GeneratorFrontType type) {
      this.edgeContext = edgeContext;
      this.type = type;
      partition = type.nodePartition.get(edgeContext.edgeId().nodeId());
    }

    @Override
    public void onStart(Consumer<Object> consumer, GlobalTime from) {
      if (partition == null) {
        consumer.accept(new UnregisterFront(edgeContext.edgeId()));
        return;
      }
      final var nexmarkConfiguration = type.nexmarkConfiguration;
      final var generatorConfig = new GeneratorConfig(
              nexmarkConfiguration,
              type.baseTime,
              1,
              type.maxEvents,
              1
      ).split(type.nodePartition.size()).get(partition);
      final var executor =
              Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, edgeContext.edgeId().toString()));
      executor.submit(() -> {
        try {
          Meta basicMeta = null;
          int childId = 0;
          final var generator = new NexmarkGenerator(generatorConfig);
          while (generator.hasNext()) {
            final var nextEvent = generator.nextEvent();
            final var event = nextEvent.event;
            final var instant = Instant.ofEpochMilli(nextEvent.eventTimestamp);
            final var sleep = Duration.between(Instant.now(), instant);
            if (!sleep.isNegative()) {
              Thread.sleep(sleep.toMillis());
            }
            final var time = Query8.tumbleStart(instant, nexmarkConfiguration.windowSizeSec)
                    - Query8.tumbleStart(Instant.ofEpochMilli(type.baseTime), nexmarkConfiguration.windowSizeSec);
            if (basicMeta != null && basicMeta.globalTime().time() > time) {
              throw new IllegalArgumentException();
            }
            if (basicMeta == null || basicMeta.globalTime().time() < time) {
              final var globalTime = new GlobalTime(time, edgeContext.edgeId());
              consumer.accept(new Heartbeat(globalTime));
              basicMeta = new Meta(globalTime);
              childId = 0;
            }
            consumer.accept(new PayloadDataItem(new Meta(basicMeta, 0, childId++), event));
          }
          consumer.accept(new Heartbeat(new GlobalTime(Long.MAX_VALUE, edgeContext.edgeId())));
          return null;
        } catch (Throwable throwable) {
          throwable.printStackTrace();
          throw throwable;
        } finally {
          executor.shutdown();
        }
      });
    }

    @Override
    public void onRequestNext() {
    }

    @Override
    public void onCheckpoint(GlobalTime to) {
    }
  }

  @Override
  public FlameRuntime.FrontInstance<Front> instance() {
    return new Instance();
  }

  @Override
  public Void handle(EdgeContext context) {
    return null;
  }
}
