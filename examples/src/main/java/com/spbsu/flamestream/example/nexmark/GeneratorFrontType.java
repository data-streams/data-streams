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
import org.apache.commons.lang3.SerializationUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Callable;
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
      final var executor =
              Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, edgeContext.edgeId().toString()));
      executor.submit(new Callable<Object>() {
        private int childId;
        private Meta basicMeta;

        @Override
        public Object call() throws Exception {
          try {
            final var warmUpRateReduction = 10;
            generate(
                    generate(
                            1,
                            type.baseTime,
                            type.nexmarkConfiguration.windowSizeSec * type.nexmarkConfiguration.firstEventRate * type.nodePartition.size(),
                            slower(type.nexmarkConfiguration, warmUpRateReduction)
                    ),
                    type.baseTime + type.nexmarkConfiguration.windowSizeSec * warmUpRateReduction * 1000,
                    type.maxEvents,
                    type.nexmarkConfiguration
            );
            consumer.accept(new Heartbeat(new GlobalTime(Long.MAX_VALUE, edgeContext.edgeId())));
            return null;
          } catch (Throwable throwable) {
            throwable.printStackTrace();
            throw throwable;
          } finally {
            executor.shutdown();
          }
        }

        private NexmarkConfiguration slower(NexmarkConfiguration nexmarkConfiguration, int times) {
          nexmarkConfiguration = SerializationUtils.clone(nexmarkConfiguration);
          nexmarkConfiguration.firstEventRate /= times;
          nexmarkConfiguration.nextEventRate /= times;
          return nexmarkConfiguration;
        }

        private long generate(
                long firstEventId,
                long baseTime,
                long maxEvents,
                NexmarkConfiguration nexmarkConfiguration
        ) throws InterruptedException {
          final var generatorConfig = new GeneratorConfig(
                  nexmarkConfiguration,
                  baseTime,
                  firstEventId,
                  maxEvents,
                  1
          );
          final var generator = new NexmarkGenerator(generatorConfig.split(type.nodePartition.size()).get(partition));
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
          System.out.println("hi");
          return generatorConfig.getStopEventId();
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
