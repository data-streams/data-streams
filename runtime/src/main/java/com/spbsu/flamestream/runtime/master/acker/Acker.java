package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.BufferedMessages;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.NodeTime;
import com.spbsu.flamestream.runtime.master.acker.api.commit.MinTimeUpdateListener;
import com.spbsu.flamestream.runtime.master.acker.api.registry.FrontTicket;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFront;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFrontFromTime;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.master.acker.table.AckTable;
import com.spbsu.flamestream.runtime.master.acker.table.ArrayAckTable;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


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
  private int acksReceived, heartbeatsReceived, nodeTimesReceived, minTimeUpdatesSent, bufferedMessagesHandled;
  private static final int SIZE = 100000;

  private NodeTimes nodeTimes = new NodeTimes();
  private final Set<ActorRef> listeners = new HashSet<>();
  private final Map<EdgeId, GlobalTime> maxHeartbeats = new HashMap<>();

  private final AckTable[] vertexTable;

  private long defaultMinimalTime;
  private final long[] vertexLastMinTime;

  private Acker(long defaultMinimalTime, boolean assertAckingBackInTime, int window, int verticesNumber) {
    vertexTable = new ArrayAckTable[verticesNumber];
    vertexLastMinTime = new long[verticesNumber];
    Arrays.fill(vertexLastMinTime, defaultMinimalTime);
    for (int i = 0; i < verticesNumber; i++) {
      vertexTable[i] = new ArrayAckTable(
              defaultMinimalTime,
              SIZE / verticesNumber,
              window,
              assertAckingBackInTime && verticesNumber == 1
      );
    }
  }

  public static Props props(long defaultMinimalTime, boolean assertAckingBackInTime, int window, int verticesNumber) {
    return Props.create(Acker.class, defaultMinimalTime, assertAckingBackInTime, window, verticesNumber)
            .withDispatcher("processing-dispatcher");
  }

  @Override
  public void postStop() throws Exception {
    try (final PrintWriter printWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(
            "/tmp/acker.txt"
    )))) {
      printWriter.println("acksReceived = " + acksReceived);
      printWriter.println("heartbeatsReceived = " + heartbeatsReceived);
      printWriter.println("nodeTimesReceived = " + nodeTimesReceived);
      printWriter.println("minTimeUpdatesSent = " + minTimeUpdatesSent);
      printWriter.println("bufferedMessagesHandled = " + bufferedMessagesHandled);
    }
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(
                    MinTimeUpdateListener.class,
                    minTimeUpdateListener -> listeners.add(minTimeUpdateListener.actorRef)
            )
            .match(NodeTime.class, nodeTime -> {
              nodeTimesReceived++;
              nodeTimes = nodeTimes.updated(nodeTime.jobaId, nodeTime.time);
            })
            .match(Ack.class, this::handleAck)
            .match(BufferedMessages.class, bufferedMessages -> {
              bufferedMessagesHandled++;
              bufferedMessages.all().forEach(receive()::apply);
            })
            .match(Heartbeat.class, this::handleHeartBeat)
            .match(RegisterFront.class, registerFront -> registerFront(registerFront.frontId()))
            .match(RegisterFrontFromTime.class, registerFront -> registerFrontFromTime(registerFront.startTime))
            .match(UnregisterFront.class, unregisterFront -> unregisterFront(unregisterFront.frontId()))
            .build();
  }

  private void registerFront(EdgeId frontId) {
    registerFrontFromTime(new GlobalTime(minAmongTables(vertexTable.length - 1), frontId));
  }

  private void registerFrontFromTime(GlobalTime startTime) {
    if (startTime.time() < minAmongTables(vertexTable.length - 1)) {
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

  private void handleHeartBeat(Heartbeat heartbeat) {
    heartbeatsReceived++;
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
    acksReceived++;
    tracer.log(ack.xor());
    if (vertexTable[ack.time().getVertexIndex()].ack(ack.time().time(), ack.xor())) {
      checkMinTime();
    }
  }

  private void checkMinTime() {
    GlobalTime minHeartbeat;
    if (maxHeartbeats.isEmpty()) {
      minHeartbeat = new GlobalTime(defaultMinimalTime, EdgeId.Min.INSTANCE);
    } else {
      minHeartbeat = Collections.min(maxHeartbeats.values());
    }
    for (int vertex = 0; vertex < vertexLastMinTime.length; vertex++) {
      final long minTime = vertexTable[vertex].tryPromote(minHeartbeat.time());
      minHeartbeat = new GlobalTime(minTime, EdgeId.Min.INSTANCE, vertex);
      if (minTime != vertexLastMinTime[vertex]) {
        vertexLastMinTime[vertex] = minTime;
        minTimeUpdatesSent++;
        log().debug("New min time: {}", minHeartbeat);
        for (final ActorRef listener : listeners) {
          listener.tell(new MinTimeUpdate(minHeartbeat, nodeTimes), self());
        }
      }
    }
  }

  private long minAmongTables(int vertex) {
    return vertexLastMinTime[vertex];
  }
}
