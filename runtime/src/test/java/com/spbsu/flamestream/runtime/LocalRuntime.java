package com.spbsu.flamestream.runtime;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.PatternsCS;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.edge.Rear;
import com.spbsu.flamestream.runtime.state.InMemStateStorage;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.typesafe.config.ConfigFactory;
import org.jetbrains.annotations.Nullable;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class LocalRuntime implements FlameRuntime {
  private final ActorSystem system;

  private final StateStorage stateStorage;
  private final int parallelism;
  private final int maxElementsInGraph;
  private final SystemConfig.Acking acking;
  private final int millisBetweenCommits;
  private final boolean barrierDisabled;
  private final boolean blinking;
  private final int blinkPeriodSec;
  private final long defaultMinimalTime;

  private LocalRuntime(
          ActorSystem system,
          int parallelism,
          int maxElementsInGraph,
          boolean barrierDisabled,
          int millisBetweenCommits,
          boolean blinking,
          int blinkPeriodSec,
          long defaultMinimalTime,
          SystemConfig.Acking acking,
          StateStorage stateStorage
  ) {
    this.parallelism = parallelism;
    this.barrierDisabled = barrierDisabled;
    this.blinking = blinking;
    this.maxElementsInGraph = maxElementsInGraph;
    this.millisBetweenCommits = millisBetweenCommits;
    this.system = system;
    this.blinkPeriodSec = blinkPeriodSec;
    this.defaultMinimalTime = defaultMinimalTime;
    this.acking = acking;
    this.stateStorage = stateStorage;
  }

  public ActorSystem system() {
    return system;
  }

  @Override
  public Flame run(Graph g) {
    final ActorRef cluster = system.actorOf(
            Cluster.props(
                    g,
                    stateStorage,
                    parallelism,
                    maxElementsInGraph,
                    barrierDisabled,
                    millisBetweenCommits,
                    acking,
                    blinking,
                    blinkPeriodSec,
                    defaultMinimalTime
            ),
            "restarter"
    );

    return new Flame() {
      @Override
      public void close() {
        system.stop(cluster);
      }

      @Override
      public <F extends Front, H> Stream<H> attachFront(String id, FrontType<F, H> type) {
        try {
          //noinspection unchecked
          return PatternsCS.ask(cluster, new FlameUmbrella.FrontTypeWithId<>(id, type), FlameConfig.config.bigTimeout())
                  .thenApply(a -> (List<H>) a).toCompletableFuture().get().stream();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public <R extends Rear, H> Stream<H> attachRear(String id, RearType<R, H> type) {
        try {
          //noinspection unchecked
          return PatternsCS.ask(cluster, new FlameUmbrella.RearTypeWithId<>(id, type), FlameConfig.config.bigTimeout())
                  .thenApply(a -> (List<H>) a).toCompletableFuture().get().stream();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  @Override
  public void close() {
    try {
      Await.ready(system.terminate(), Duration.Inf());
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public static class Builder {
    private int parallelism = DEFAULT_PARALLELISM;
    private int maxElementsInGraph = DEFAULT_MAX_ELEMENTS_IN_GRAPH;
    private int millisBetweenCommits = DEFAULT_MILLIS_BETWEEN_COMMITS;
    private boolean blinking = false;
    private int blinkPeriodSec = 10;
    private SystemConfig.Acking acking = SystemConfig.Acking.CENTRALIZED;
    private StateStorage stateStorage = new InMemStateStorage();
    private long defaultMinimalTime = 0;

    @Nullable
    private ActorSystem system = null;
    private boolean barrierDisabled;

    public Builder parallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder maxElementsInGraph(int maxElementsInGraph) {
      this.maxElementsInGraph = maxElementsInGraph;
      return this;
    }

    public Builder withBlink() {
      this.blinking = true;
      return this;
    }

    public Builder blinkPeriodSec(int blinkPeriodSec) {
      this.blinkPeriodSec = blinkPeriodSec;
      return this;
    }

    public Builder defaultMinimalTime(long defaultMinimalTime) {
      this.defaultMinimalTime = defaultMinimalTime;
      return this;
    }

    public Builder withStateStorage(StateStorage stateStorage) {
      this.stateStorage = stateStorage;
      return this;
    }

    public Builder millisBetweenCommits(int millisBetweenCommits) {
      this.millisBetweenCommits = millisBetweenCommits;
      return this;
    }

    public Builder withSystem(ActorSystem system) {
      this.system = system;
      return this;
    }

    public Builder acking(SystemConfig.Acking acking) {
      this.acking = acking;
      return this;
    }

    public Builder barrierDisabled(boolean barrierDisabled) {
      this.barrierDisabled = barrierDisabled;
      return this;
    }

    public LocalRuntime build() {
      if (system == null) {
        system = ActorSystem.create("local-runtime", ConfigFactory.load("local"));
      }
      return new LocalRuntime(
              system,
              parallelism,
              maxElementsInGraph,
              barrierDisabled,
              millisBetweenCommits,
              blinking,
              blinkPeriodSec,
              defaultMinimalTime,
              acking,
              stateStorage
      );
    }
  }
}
