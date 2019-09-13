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
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.akka.PingActor;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LocalAcker extends LoggingActor {
  public static class Partitions {
    final int size;

    public Partitions(int size) {this.size = size;}

    public long partitionTime(int partition, long time) {
      return Math.floorDiv(time + size - 1 - partition, size) * size + partition;
    }

    public int timePartition(long time) {
      return (int) Math.floorMod(time, size);
    }
  }

  private static class HeartbeatIncrease {
    long previous = Long.MIN_VALUE, current = Long.MIN_VALUE;
  }

  private final int flushDelayInMillis;
  private final int flushCount;

  private long nodeTime = Long.MIN_VALUE;
  private final SortedMap<GlobalTime, Long> ackCache = new TreeMap<>(Comparator.reverseOrder());
  private final Map<EdgeId, HeartbeatIncrease> edgeIdHeartbeatIncrease = new HashMap<>();
  private final List<UnregisterFront> unregisterCache = new ArrayList<>();

  private final List<ActorRef> ackers;
  private final Partitions partitions;
  private final List<ActorRef> listeners = new ArrayList<>();
  private final MinTimeUpdater minTimeUpdater;
  private final String nodeId;
  private final ActorRef pingActor;
  private long acksSent = 0;
  private long heartbeatsSent = 0;

  private int flushCounter = 0;

  public LocalAcker(List<ActorRef> ackers, String nodeId, int flushDelayInMillis, int flushCount) {
    this.ackers = ackers;
    partitions = new Partitions(ackers.size());
    minTimeUpdater = new MinTimeUpdater(ackers);
    this.nodeId = nodeId;
    pingActor = context().actorOf(PingActor.props(self(), Flush.FLUSH));
    this.flushDelayInMillis = flushDelayInMillis;
    this.flushCount = flushCount;
  }

  public static class Builder {
    private int flushDelayInMillis = 5;
    private int flushCount = 1000;

    public Props props(List<ActorRef> ackers, String nodeId) {
      return Props.create(LocalAcker.class, ackers, nodeId, flushDelayInMillis, flushCount)
              .withDispatcher("processing-dispatcher");
    }

    public Builder flushDelayInMillis(int flushDelayInMillis) {
      this.flushDelayInMillis = flushDelayInMillis;
      return this;
    }

    public Builder flushCount(int flushCount) {
      this.flushCount = flushCount;
      return this;
    }
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    minTimeUpdater.subscribe(self());
    pingActor.tell(new PingActor.Start(TimeUnit.MILLISECONDS.toNanos(flushDelayInMillis)), self());
  }

  @Override
  public void postStop() throws Exception {
    pingActor.tell(new PingActor.Stop(), self());
    try (final PrintWriter printWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(
            "/tmp/local_acker.txt"
    )))) {
      printWriter.println("nodeTime = " + (nodeTime - Long.MIN_VALUE));
      printWriter.println("acksSent = " + acksSent);
      printWriter.println("heartbeatsSent = " + heartbeatsSent);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Flush.class, flush -> flush())
            .match(Heartbeat.class, this::receiveHeartbeat)
            .match(UnregisterFront.class, unregisterCache::add)
            .match(MinTimeUpdateListener.class, minTimeUpdateListener -> listeners.add(minTimeUpdateListener.actorRef))
            .match(MinTimeUpdate.class, minTimeUpdate -> {
              @Nullable MinTimeUpdate minTime = minTimeUpdater.onShardMinTimeUpdate(sender(), minTimeUpdate);
              if (minTime != null) {
                listeners.forEach(listener -> listener.tell(minTime, self()));
              }
            })
            .matchAny(e -> ackers.forEach(acker -> acker.forward(e, context())))
            .build();
  }

  private void receiveHeartbeat(Heartbeat heartbeat) {
    final HeartbeatIncrease heartbeatIncrease = edgeIdHeartbeatIncrease.computeIfAbsent(
            heartbeat.time().frontId(),
            __ -> new HeartbeatIncrease()
    );
    heartbeatIncrease.current = Math.max(heartbeatIncrease.current, heartbeat.time().time());
  }

  private void tick() {
    if (flushCounter == flushCount) {
      flush();
    } else {
      flushCounter++;
    }
  }

  private void handleAck(Ack ack) {
    ackCache.compute(ack.time(), (globalTime, xor) -> {
      if (xor == null) {
        return ack.xor();
      } else {
        return ack.xor() ^ xor;
      }
    });

    tick();
  }

  private void flush() {
    final Map<ActorRef, List<Object>> ackerBufferedMessages = ackers.stream().collect(Collectors.toMap(
            Function.identity(),
            __ -> new ArrayList<>()
    ));
    final boolean acksEmpty = ackCache.isEmpty();
    if (!acksEmpty && ackers.size() > 1) {
      final NodeTime nodeTime = new NodeTime(nodeId, this.nodeTime);
      ackers.forEach(acker -> ackerBufferedMessages.get(acker).add(nodeTime));
    }
    nodeTime++;

    ackCache.entrySet()
            .stream()
            .map(entry -> new Ack(entry.getKey(), entry.getValue()))
            .collect(Collectors.groupingBy(o -> ackers.get(partitions.timePartition(o.time().time()))))
            .forEach((acker, acks) -> ackerBufferedMessages.get(acker).addAll(acks));
    ackCache.clear();

    edgeIdHeartbeatIncrease.forEach((edgeId, heartbeatIncrease) -> {
      IntStream.range(0, ackers.size()).forEach(partition -> {
        long current = partitions.partitionTime(partition, heartbeatIncrease.current);
        if (partitions.partitionTime(partition, heartbeatIncrease.previous) < current) {
          ackerBufferedMessages.get(ackers.get(partition)).add(new Heartbeat(new GlobalTime(current, edgeId)));
        }
      });
      heartbeatIncrease.previous = heartbeatIncrease.current;
    });

    if (acksEmpty) {
      ackers.forEach(acker -> ackerBufferedMessages.get(acker).addAll(unregisterCache));
      unregisterCache.clear();
    }

    ackerBufferedMessages.forEach((acker, messages) -> {
      if (!ackerBufferedMessages.isEmpty()) {
        acker.tell(new BufferedMessages(messages), self());
      }
    });
    flushCounter = 0;
  }

  private enum Flush {
    FLUSH
  }
}
