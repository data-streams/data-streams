package com.spbsu.flamestream.runtime;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.EdgeManager;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.graph.GraphManager;
import com.spbsu.flamestream.runtime.edge.negitioator.Negotiator;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.HashMap;
import java.util.Map;

public class FlameNode extends LoggingActor {
  private final ActorRef edgeManager;
  private final ClusterConfig config;

  private FlameNode(String id,
                    Graph bootstrapGraph,
                    ClusterConfig config,
                    ActorRef localAcker,
                    ActorRef registryHolder,
                    ActorRef committer,
                    int maxElementsInGraph,
                    boolean barrierIsDisabled,
                    StateStorage storage) {
    this.config = config;

    final ActorRef graph = context().actorOf(GraphManager.props(
            id,
            bootstrapGraph,
            localAcker,
            registryHolder,
            committer,
            new ComputationProps(config.hashGroups(), maxElementsInGraph, barrierIsDisabled),
            storage
    ), "graph");
    graph.tell(resolvedManagers(), self());

    final ActorRef negotiator = context().actorOf(Negotiator.props(registryHolder, graph), "negotiator");
    this.edgeManager = context().actorOf(EdgeManager.props(config.paths().get(id), id, negotiator, graph), "edge");
  }

  @Override
  public void preStart() {
    log().info("Starting FlameNode with config '{}'", config);
  }

  public static Props props(String id,
                            Graph initialGraph,
                            ClusterConfig initialConfig,
                            ActorRef localAcker,
                            ActorRef registryHolder,
                            ActorRef committer,
                            int maxElementsInGraph,
                            boolean barrierIsDisabled,
                            StateStorage stateStorage) {
    return Props.create(
            FlameNode.class,
            id,
            initialGraph,
            initialConfig,
            localAcker,
            registryHolder,
            committer,
            maxElementsInGraph,
            barrierIsDisabled,
            stateStorage
    );
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AttachFront.class, f -> edgeManager.forward(f, context()))
            .match(AttachRear.class, f -> edgeManager.forward(f, context()))
            .build();
  }

  private Map<String, ActorRef> resolvedManagers() {
    final Map<String, ActorRef> managers = new HashMap<>();
    config.paths().forEach((id, path) -> {
      final ActorRef manager = AwaitResolver.syncResolve(path.child("graph"), context());
      managers.put(id, manager);
    });
    return managers;
  }
}
