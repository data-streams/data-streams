package com.spbsu.flamestream.runtime;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.HashGroup;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.config.ZookeeperWorkersNode;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.master.acker.Committer;
import com.spbsu.flamestream.runtime.master.acker.MinTimeUpdater;
import com.spbsu.flamestream.runtime.master.acker.RegistryHolder;
import com.spbsu.flamestream.runtime.master.acker.ZkRegistry;
import com.spbsu.flamestream.runtime.serialization.FlameSerializer;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ProcessingWatcher extends LoggingActor {
  private final String id;
  private final CuratorFramework curator;
  private final ZookeeperWorkersNode zookeeperWorkersNode;
  private final SystemConfig systemConfig;
  private final StateStorage stateStorage;
  private final FlameSerializer serializer;

  private NodeCache graphCache = null;
  private PathChildrenCache frontsCache = null;
  private PathChildrenCache rearsCache = null;

  private ActorRef flameNode = null;
  private Graph graph = null;

  public ProcessingWatcher(String id,
                           CuratorFramework curator,
                           ZookeeperWorkersNode zookeeperWorkersNode,
                           SystemConfig systemConfig,
                           StateStorage stateStorage,
                           FlameSerializer serializer
  ) {
    this.id = id;
    this.curator = curator;
    this.zookeeperWorkersNode = zookeeperWorkersNode;
    this.systemConfig = systemConfig;
    this.stateStorage = stateStorage;
    this.serializer = serializer;
  }

  public static Props props(String id,
                            CuratorFramework curator,
                            ZookeeperWorkersNode zookeeperWorkersNode,
                            SystemConfig systemConfig,
                            StateStorage stateStorage,
                            FlameSerializer serializer
  ) {
    return Props.create(
            ProcessingWatcher.class,
            id,
            curator,
            zookeeperWorkersNode,
            systemConfig,
            stateStorage,
            serializer
    );
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();

    graphCache = new NodeCache(curator, "/graph");
    graphCache.getListenable().addListener(() -> {
      if (graphCache.getCurrentData().getData().length > 0) {
        self().tell(serializer.deserialize(graphCache.getCurrentData().getData(), Graph.class), self());
      }
    });
    graphCache.start();

    final Stat stat = curator.checkExists().usingWatcher((CuratorWatcher) watchedEvent -> {
      if (watchedEvent.getType() == Watcher.Event.EventType.NodeCreated) {
        startEdgeCaches();
      }
    }).forPath("/graph");
    if (stat != null) {
      startEdgeCaches();
    }
  }

  @Override
  public void postStop() throws Exception {
    //noinspection EmptyTryBlock,unused
    try (
            NodeCache gc = graphCache;
            PathChildrenCache fc = frontsCache;
            PathChildrenCache rc = rearsCache
    ) {

    } catch (IOException e) {
      e.printStackTrace();
    }
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Graph.class, this::onGraph)
            .matchAny(o -> stash())
            .build();
  }

  private Receive running() {
    return ReceiveBuilder.create()
            .match(AttachFront.class, attachFront -> flameNode.tell(attachFront, self()))
            .match(AttachRear.class, attachRear -> flameNode.tell(attachRear, self()))
            .build();
  }

  private void onGraph(Graph graph) {
    if (this.graph != null) {
      throw new RuntimeException("Graph updating is not supported yet");
    }
    try {
      PatternsCS.ask(context().actorOf(InitAgent.props()), graph, FlameConfig.config.bigTimeout())
              .toCompletableFuture()
              .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    this.graph = graph;
    final List<ZookeeperWorkersNode.Worker> workers = zookeeperWorkersNode.workers();
    final ClusterConfig config = ClusterConfig.fromWorkers(workers);
    final List<ActorRef> ackers = ackers(config);
    final @Nullable ActorRef localAcker = ackers.isEmpty() ? null : context().actorOf(
            systemConfig.getLocalAckerBuilder().props(ackers, id)
    );
    final ActorRef committer, registryHolder;
    if (systemConfig.workersResourcesDistributor.master(config.ids).equals(id)) {
      registryHolder = context().actorOf(
              RegistryHolder.props(new ZkRegistry(curator), ackers, systemConfig.defaultMinimalTime()),
              "registry-holder"
      );
      committer = context().actorOf(Committer.props(
              config.paths().size(),
              systemConfig,
              registryHolder,
              new MinTimeUpdater(ackers)
      ), "committer");
    } else {
      final ActorPath masterPath = config.paths().get(config.masterLocation()).child("processing-watcher");
      registryHolder = AwaitResolver.syncResolve(masterPath.child("registry-holder"), context());
      committer = AwaitResolver.syncResolve(masterPath.child("committer"), context());
    }

    if (ackers.isEmpty() && !systemConfig.barrierIsDisabled()) {
      throw new IllegalArgumentException("barrier should be disabled when acking is");
    }
    this.flameNode = context().actorOf(
            FlameNode.props(
                    id,
                    graph,
                    config.withChildPath("processing-watcher").withChildPath("graph"),
                    systemConfig.workersResourcesDistributor.hashGroups(config.paths().keySet()),
                    localAcker,
                    registryHolder,
                    committer,
                    systemConfig.maxElementsInGraph(),
                    systemConfig.barrierIsDisabled(),
                    stateStorage
            ),
            "graph"
    );

    unstashAll();
    getContext().become(running());
  }

  private void startEdgeCaches() throws Exception {
    rearsCache = new PathChildrenCache(curator, "/graph/rears", false);
    rearsCache.getListenable().addListener((client, event) -> {
      if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
        final FlameRuntime.RearInstance<?> rear = serializer.deserialize(
                curator.getData().forPath(event.getData().getPath()),
                FlameRuntime.RearInstance.class
        );
        self().tell(
                new AttachRear<>(StringUtils.substringAfterLast(event.getData().getPath(), "/"), rear),
                self()
        );
      }
    });
    rearsCache.start();

    frontsCache = new PathChildrenCache(curator, "/graph/fronts", false);
    frontsCache.getListenable().addListener((client, event) -> {
      if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
        final FlameRuntime.FrontInstance<?> front = serializer.deserialize(
                curator.getData().forPath(event.getData().getPath()),
                FlameRuntime.FrontInstance.class
        );
        self().tell(
                new AttachFront<>(StringUtils.substringAfterLast(event.getData().getPath(), "/"), front),
                self()
        );
      }
    });
    frontsCache.start();
  }

  private static class InitAgent extends LoggingActor {
    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(Graph.class, graph -> {
                graph.components().forEach(vertexStream -> vertexStream.forEach(vertex -> {
                  if (vertex instanceof FlameMap) {
                    ((FlameMap) vertex).init();
                  }
                }));
                sender().tell(InitDone.OBJECT, self());
                context().stop(self());
              })
              .build();
    }

    public static Props props() {
      return Props.create(InitAgent.class);
    }
  }

  private enum InitDone {
    OBJECT
  }

  private List<ActorRef> ackers(ClusterConfig config) {
    {
      final List<CompletableFuture<ActorRef>> ackerFutures = systemConfig
              .workersResourcesDistributor.ackers(config.ids).stream().map(config.paths()::get)
              .map(actorPath -> AwaitResolver.resolve(actorPath.child("acker"), context()).toCompletableFuture())
              .collect(Collectors.toList());
      ackerFutures.forEach(CompletableFuture::join);
      return ackerFutures.stream().map(future -> {
        try {
          return future.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }).collect(Collectors.toList());
    }
  }
}
