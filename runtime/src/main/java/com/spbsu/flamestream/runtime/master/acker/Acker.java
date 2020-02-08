package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.BufferedMessages;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.NodeTime;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.MinTimeUpdateListener;
import com.spbsu.flamestream.runtime.master.acker.api.registry.FrontTicket;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFront;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFrontFromTime;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.master.acker.table.AckTable;
import com.spbsu.flamestream.runtime.master.acker.table.ArrayAckTable;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;


/**
 * <h3>Actor Contract</h3>
 * <h4>Inbound Messages</h4>
 * <ol>
 * <li>{@link RegisterFront} requests to add frontClass to the supervision</li>
 * <li>{@link Ack} acks</li>
 * <li>{@link BufferedMessages} cached acks from local acker</li>
 * <li>{@link Heartbeat} heartbeats</li>
 * </ol>
 * <h4>Outbound Messages</h4>
 * <ol>
 * <li>{@link FrontTicket} - reply to the frontClass registration request. Sets the lowest allowed timestamp</li>
 * <li>{@link MinTimeUpdate} mintime</li>
 * </ol>
 * <h4>Failure Modes</h4>
 * <ol>
 * <li>{@link RuntimeException} - if something goes wrong</li>
 * </ol>
 */
public class Acker extends LoggingActor {
  private static final int WINDOW = 1;
  private static final int SIZE = 100000;
  private final ComponentAckTable sinkComponent;

  private NodeTimes nodeTimes = new NodeTimes();
  private final Set<ActorRef> listeners = new HashSet<>();
  private final Map<EdgeId, GlobalTime> maxHeartbeats = new HashMap<>();

  private final ComponentAckTable[] componentTable;
  private long defaultMinimalTime;

  private static class ComponentAckTable {
    final TrackingComponent component;
    final AckTable ackTable;
    long lastMinTime;

    private ComponentAckTable(TrackingComponent component, AckTable ackTable, long defaultMinimialTime) {
      this.component = component;
      this.ackTable = ackTable;
      lastMinTime = defaultMinimialTime;
    }
  }

  private Acker(long defaultMinimalTime, boolean assertAckingBackInTime, Graph graph) {
    int componentsNumber = 0;
    final Iterable<Graph.Vertex> vertices = () -> graph.components().flatMap(Function.identity()).iterator();
    for (Graph.Vertex component : vertices) {
      componentsNumber = Integer.max(componentsNumber, component.trackingComponent().index + 1);
    }
    componentTable = new ComponentAckTable[componentsNumber];
    for (final Graph.Vertex vertex : vertices) {
      if (componentTable[vertex.trackingComponent().index] == null) {
        componentTable[vertex.trackingComponent().index] = new ComponentAckTable(
                vertex.trackingComponent(),
                new ArrayAckTable(defaultMinimalTime, SIZE / componentsNumber, WINDOW, assertAckingBackInTime),
                defaultMinimalTime
        );
      }
    }
    sinkComponent = componentTable[componentTable.length - 1];
    this.defaultMinimalTime = defaultMinimalTime;
  }

  public static Props props(long defaultMinimalTime, boolean assertAckingBackInTime, Graph graph) {
    return Props.create(Acker.class, defaultMinimalTime, assertAckingBackInTime, graph)
            .withDispatcher("processing-dispatcher");
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(MinTimeUpdateListener.class, minTimeUpdateListener -> listeners.add(minTimeUpdateListener.actorRef))
            .match(NodeTime.class, nodeTime -> nodeTimes = nodeTimes.updated(nodeTime.jobaId, nodeTime.time))
            .match(Ack.class, this::handleAck)
            .match(BufferedMessages.class, bufferedMessages -> bufferedMessages.all().forEach(receive()::apply))
            .match(Heartbeat.class, this::handleHeartBeat)
            .match(RegisterFront.class, registerFront -> registerFront(registerFront.frontId()))
            .match(RegisterFrontFromTime.class, registerFront -> registerFrontFromTime(registerFront.startTime))
            .match(UnregisterFront.class, unregisterFront -> unregisterFront(unregisterFront.frontId()))
            .build();
  }

  private void registerFront(EdgeId frontId) {
    registerFrontFromTime(new GlobalTime(sinkComponent.lastMinTime, frontId));
  }

  private void registerFrontFromTime(GlobalTime startTime) {
    if (startTime.time() < sinkComponent.lastMinTime) {
      throw new RuntimeException("Registering front back in time");
    }
    maxHeartbeats.put(startTime.frontId(), startTime);
    sender().tell(new FrontTicket(startTime), self());
  }

  private void unregisterFront(EdgeId frontId) {
    log().info("Unregistering front {}", frontId);
    final GlobalTime removed = maxHeartbeats.remove(frontId);
    if (removed == null) {
      log().warning("Front " + frontId + " has been already unregistered");
    } else {
      defaultMinimalTime = Math.max(defaultMinimalTime, removed.time());
    }
    checkMinTime();
  }

  private void checkMinTime() {
    checkMinTime(componentTable[0].component);
  }

  private void handleHeartBeat(Heartbeat heartbeat) {
    final GlobalTime time = heartbeat.time();
    final GlobalTime previousHeartbeat = maxHeartbeats.get(heartbeat.time().frontId());
    if (heartbeat.time().compareTo(previousHeartbeat) < 0) {
      throw new IllegalStateException("Non monotonic heartbeats");
    }
    maxHeartbeats.put(time.frontId(), heartbeat.time());
    checkMinTime();
  }

  private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("ack-receive");

  private void handleAck(Ack ack) {
    tracer.log(ack.xor());
    final ComponentAckTable table = componentTable[ack.trackingComponent()];
    if (table.ackTable.ack(ack.time().time(), ack.xor())) {
      checkMinTime(table.component);
    }
  }

  private void checkMinTime(TrackingComponent component) {
    final long minHeartbeat =
            maxHeartbeats.isEmpty() ? defaultMinimalTime : Collections.min(maxHeartbeats.values()).time();
    final TreeSet<TrackingComponent> componentsToRefresh = new TreeSet<>();
    componentsToRefresh.add(component);
    while ((component = componentsToRefresh.pollFirst()) != null) {
      final ComponentAckTable table = componentTable[component.index];
      final long minTime = table.ackTable.tryPromote(minHeartbeat);
      if (minTime == table.lastMinTime) {
        continue;
      }
      componentsToRefresh.addAll(component.inbound);
      table.lastMinTime = minTime;
      log().debug("New min time: {}", minHeartbeat);
      final GlobalTime minAmongTables = new GlobalTime(minTime, EdgeId.MIN);
      for (final ActorRef listener : listeners) {
        listener.tell(new MinTimeUpdate(table.component.index, minAmongTables, nodeTimes), self());
      }
    }
  }
}
