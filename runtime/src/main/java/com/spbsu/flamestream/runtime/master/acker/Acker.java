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

  private final AckTable table;

  private long defaultMinimalTime;
  private GlobalTime lastMinTime = GlobalTime.MIN;

  private Acker(long defaultMinimalTime, boolean assertAckingBackInTime, int window) {
    table = new ArrayAckTable(defaultMinimalTime, SIZE, window, assertAckingBackInTime);
  }

  public static Props props(long defaultMinimalTime, boolean assertAckingBackInTime, int window) {
    return Props.create(Acker.class, defaultMinimalTime, assertAckingBackInTime, window)
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
    registerFrontFromTime(new GlobalTime(minAmongTables().time(), frontId));
  }

  private void registerFrontFromTime(GlobalTime startTime) {
    if (startTime.compareTo(minAmongTables()) < 0) {
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
    if (table.ack(ack.time().time(), ack.xor())) {
      checkMinTime();
    }
  }

  private void checkMinTime() {
    final GlobalTime minAmongTables = minAmongTables();
    if (minAmongTables.compareTo(lastMinTime) > 0) {
      minTimeUpdatesSent++;
      this.lastMinTime = minAmongTables;
      log().debug("New min time: {}", lastMinTime);
      listeners.forEach(s -> s.tell(new MinTimeUpdate(lastMinTime, nodeTimes), self()));
    }
  }

  private GlobalTime minAmongTables() {
    final GlobalTime minHeartbeat;
    if (maxHeartbeats.isEmpty()) {
      minHeartbeat = new GlobalTime(defaultMinimalTime, EdgeId.Min.INSTANCE);
    } else {
      minHeartbeat = Collections.min(maxHeartbeats.values());
    }
    final long minTime = table.tryPromote(minHeartbeat.time());
    return new GlobalTime(minTime, EdgeId.Min.INSTANCE);
  }
}
