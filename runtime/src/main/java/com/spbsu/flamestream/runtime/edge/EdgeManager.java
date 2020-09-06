package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.graph.api.NewRear;
import com.spbsu.flamestream.runtime.edge.negitioator.api.NewFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

public class EdgeManager extends LoggingActor {
  private final ActorPath nodePath;
  private final String nodeId;
  private final ActorRef negotiator;
  private final ActorRef graphManager;

  private EdgeManager(ActorPath nodePath, String nodeId, ActorRef localNegotiator, ActorRef graphManager) {
    this.nodePath = nodePath;
    this.nodeId = nodeId;
    this.negotiator = localNegotiator;
    this.graphManager = graphManager;
  }

  public static Props props(ActorPath nodePath, String nodeId, ActorRef localNegotiator, ActorRef graphManager) {
    return Props.create(EdgeManager.class, nodePath, nodeId, localNegotiator, graphManager);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AttachFront.class, attachFront -> {
              final ActorRef frontRef = context().actorOf(FrontActor.props(
                      new SystemEdgeContext(nodePath, nodeId, attachFront.id()),
                      attachFront.instance(),
                      attachFront.trackingWindow
              ), attachFront.id());
              negotiator.tell(new NewFront(new EdgeId(attachFront.id(), nodeId), frontRef), self());
            })
            .match(AttachRear.class, attachRear -> {
              final ActorRef rearRef = context().actorOf(RearActor.props(
                      new SystemEdgeContext(nodePath, nodeId, attachRear.id()),
                      attachRear.instance()
              ), attachRear.id());
              graphManager.tell(new NewRear(rearRef), self());
            })
            .build();
  }
}
