package com.spbsu.flamestream.runtime;

import akka.actor.ActorKilledException;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Kill;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.RootActorPath;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.config.HashGroup;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.edge.Front;
import com.spbsu.flamestream.runtime.edge.Rear;
import com.spbsu.flamestream.runtime.edge.SystemEdgeContext;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.master.acker.Acker;
import com.spbsu.flamestream.runtime.master.acker.Committer;
import com.spbsu.flamestream.runtime.master.acker.LocalAcker;
import com.spbsu.flamestream.runtime.master.acker.MinTimeUpdater;
import com.spbsu.flamestream.runtime.master.acker.Registry;
import com.spbsu.flamestream.runtime.master.acker.RegistryHolder;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

class Cluster extends LoggingActor {
  private final ActorRef inner;
  private final boolean blinking;
  private final int blinkPeriodSec;

  private Cluster(
          Graph g,
          StateStorage stateStorage,
          int parallelism,
          int maxElementsInGraph,
          boolean barrierDisabled,
          int millisBetweenCommits,
          SystemConfig.Acking acking,
          boolean blinking,
          int blinkPeriodSec
  ) {
    this.blinking = blinking;
    this.blinkPeriodSec = blinkPeriodSec;

    final Map<String, HashGroup> ranges = new HashMap<>();
    final Map<String, ActorPath> paths = new HashMap<>();
    final List<HashUnit> ra = HashUnit.covering(parallelism).collect(Collectors.toList());
    for (int i = 0; i < parallelism; ++i) {
      final String id = "node-" + i;
      final HashUnit range = ra.get(i);
      paths.put(
              id,
              RootActorPath.apply(context().system().provider().getDefaultAddress(), "/")
                      .child("user")
                      .child("restarter")
                      .child("cluster")
                      .child(id)
      );
      ranges.put(id, new HashGroup(Collections.singleton(range)));
    }
    final ClusterConfig clusterConfig = new ClusterConfig(paths, "node-0");
    final int defaultMinimalTime = 0;
    final SystemConfig systemConfig =
            new SystemConfig(
                    maxElementsInGraph,
                    millisBetweenCommits,
                    defaultMinimalTime,
                    acking,
                    barrierDisabled,
                    new LocalAcker.Builder()
            );

    final Registry registry = new InMemoryRegistry();
    inner = context().actorOf(FlameUmbrella.props(
            context -> {
              final List<ActorRef> ackers;
              switch (acking) {
                case DISABLED:
                  ackers = Collections.emptyList();
                  break;
                case CENTRALIZED:
                  ackers = Collections.singletonList(context.actorOf(Acker.props(defaultMinimalTime, true), "acker"));
                  break;
                case DISTRIBUTED:
                  ackers = paths.keySet()
                          .stream()
                          .map(id -> context.actorOf(Acker.props(defaultMinimalTime, false), "acker-" + id))
                          .collect(Collectors.toList());
                  break;
                default:
                  throw new IllegalStateException("Unexpected value: " + acking);
              }
              final ActorRef localAcker = ackers.isEmpty() ? null : context.actorOf(
                      systemConfig.getLocalAckerBuilder().props(ackers, ""),
                      "localAcker"
              );
              final ActorRef registryHolder = context.actorOf(
                      RegistryHolder.props(registry, ackers, defaultMinimalTime),
                      "registry-holder"
              );
              final ActorRef committer = context.actorOf(Committer.props(
                      clusterConfig.paths().size(),
                      systemConfig,
                      registryHolder,
                      new MinTimeUpdater(ackers)
              ));
              return paths.keySet().stream().map(id -> context.actorOf(FlameNode.props(
                      id,
                      g,
                      clusterConfig,
                      ranges,
                      localAcker,
                      registryHolder,
                      committer,
                      maxElementsInGraph,
                      barrierDisabled,
                      stateStorage
              ), id)).collect(Collectors.toList());
            },
            paths, g
    ), "cluster");
  }

  static Props props(Graph g,
                     StateStorage stateStorage,
                     int parallelism,
                     int maxElementsInGraph,
                     boolean barrierDisabled,
                     int millisBetweenCommits,
                     SystemConfig.Acking acking,
                     boolean blinking,
                     int blinkPeriodSec
  ) {
    return Props.create(
            Cluster.class,
            g,
            stateStorage,
            parallelism,
            maxElementsInGraph,
            barrierDisabled,
            millisBetweenCommits,
            acking,
            blinking,
            blinkPeriodSec
    );
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .matchAny(m -> inner.forward(m, context()))
            .build();
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    if (blinking) {
      context().system()
              .scheduler()
              .schedule(
                      FiniteDuration.apply(blinkPeriodSec, TimeUnit.SECONDS),
                      FiniteDuration.apply(blinkPeriodSec, TimeUnit.SECONDS),
                      inner,
                      Kill.getInstance(),
                      context().system().dispatcher(),
                      self()
              );
    }
  }

  private static final SupervisorStrategy strategy =
          new OneForOneStrategy(100000, Duration.create(1, TimeUnit.MINUTES), DeciderBuilder
                  .match(ActorKilledException.class, e -> SupervisorStrategy.restart())
                  .matchAny(o -> SupervisorStrategy.escalate()).build());

  @Override
  public SupervisorStrategy supervisorStrategy() {
    return strategy;
  }
}

class FlameUmbrella extends LoggingActor {
  private final List<Object> toBeTold;
  private final Map<String, ActorPath> paths;
  private final Iterable<ActorRef> flameNodes;

  private FlameUmbrella(Function<akka.actor.ActorContext, Iterable<ActorRef>> actorsStarter,
                        Map<String, ActorPath> paths,
                        List<Object> toBeTold, Graph graph) {
    this.paths = paths;
    this.toBeTold = toBeTold;

    graph.components().forEach(vertexStream -> vertexStream.forEach(vertex -> {
      if (vertex instanceof FlameMap) {
        ((FlameMap) vertex).init();
      }
    }));
    flameNodes = actorsStarter.apply(context());

    // Reattach rears first
    toBeTold.stream().filter(e -> e instanceof AttachRear).forEach(a -> flameNodes.forEach(c -> c.tell(a, self())));
    toBeTold.stream().filter(e -> e instanceof AttachFront).forEach(a -> flameNodes.forEach(c -> {
      c.tell(a, self());
      // reproduce front delay
      try {
        Thread.sleep(FlameConfig.config.smallTimeout().duration().toMillis());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }));
  }

  static Props props(Function<akka.actor.ActorContext, Iterable<ActorRef>> flameNodesStarter,
                     Map<String, ActorPath> paths, Graph graph) {
    return Props.create(FlameUmbrella.class, flameNodesStarter, paths, new ArrayList<>(), graph);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(FrontTypeWithId.class, a -> {
              //noinspection unchecked
              final AttachFront attach = new AttachFront<>(a.id, a.type.instance());
              toBeTold.add(attach);
              flameNodes.forEach(n -> n.tell(attach, self()));
              final List<Object> collect = paths.entrySet().stream()
                      .map(node -> a.type.handle(new SystemEdgeContext(node.getValue(), node.getKey(), a.id)))
                      .collect(Collectors.toList());
              sender().tell(collect, self());
            })
            .match(RearTypeWithId.class, a -> {
              //noinspection unchecked
              final AttachRear attach = new AttachRear(a.id, a.type.instance());
              toBeTold.add(attach);
              flameNodes.forEach(n -> n.tell(attach, self()));
              final List<Object> collect = paths.entrySet().stream()
                      .map(node -> a.type.handle(new SystemEdgeContext(node.getValue(), node.getKey(), a.id)))
                      .collect(Collectors.toList());
              sender().tell(collect, self());
            })
            .build();
  }

  static class FrontTypeWithId<F extends Front, H> {
    final String id;
    final FlameRuntime.FrontType<F, H> type;

    FrontTypeWithId(String id, FlameRuntime.FrontType<F, H> type) {
      this.id = id;
      this.type = type;
    }
  }

  static class RearTypeWithId<R extends Rear, H> {
    final String id;
    final FlameRuntime.RearType<R, H> type;

    RearTypeWithId(String id, FlameRuntime.RearType<R, H> type) {
      this.id = id;
      this.type = type;
    }
  }
}


class InMemoryRegistry implements Registry {
  private final Map<EdgeId, Long> linearizableCollection = new HashMap<>();
  private long lastCommit = 0;

  @Override
  public Map<EdgeId, Long> registeredFronts() {
    return new HashMap<>(linearizableCollection);
  }

  @Override
  public void register(EdgeId frontId, long attachTimestamp) {
    linearizableCollection.compute(frontId, (edgeId, time) -> {
      if (time != null) {
        throw new IllegalArgumentException("Front " + frontId + " has been already registered");
      }
      return attachTimestamp;
    });
  }

  @Override
  public long registeredTime(EdgeId frontId) {
    return linearizableCollection.getOrDefault(frontId, -1L);
  }

  @Override
  public void committed(long time) {
    if (time < lastCommit) {
      throw new IllegalArgumentException("Not monotonic last commit time, expected " + time + " < " + lastCommit);
    }
    lastCommit = time;
  }

  @Override
  public long lastCommit() {
    return lastCommit;
  }
}
