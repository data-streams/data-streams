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
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Integer.parseInt;

public class LocalAcker extends LoggingActor {
  private static int additionalAcks = System.getenv().containsKey("LOCAL_ACKER_ACKS_MULTIPLIER") ?
          parseInt(System.getenv("LOCAL_ACKER_ACKS_MULTIPLIER")) : 1;

  public static class Partitions {
    final int size;

    public Partitions(int size) {this.size = size;}

    public long partitionTime(int partition, int hash, long time) {
      return Math.floorDiv(hash + time + size - 1 - partition, size) * size - hash + partition;
    }

    public int timePartition(int hash, long time) {
      return (int) Math.floorMod(hash + time, size);
    }
  }

  private static class HeartbeatIncrease {
    long previous = Long.MIN_VALUE, current = Long.MIN_VALUE;
  }

  private final int flushDelayInMillis;
  private final int flushCount;

  private long nodeTime = Long.MIN_VALUE;
  private final SortedMap<GlobalTime, Long> ackCache = new TreeMap<>(Comparator.reverseOrder());
  private boolean bumpNodeTimes;
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
            .match(UnregisterFront.class, unregisterFront -> {
              if (flushCount == 0) {
                ackers.forEach(acker -> acker.tell(unregisterFront, self()));
              } else {
                unregisterCache.add(unregisterFront);
              }
            })
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
    final GlobalTime time = heartbeat.time();
    final HeartbeatIncrease heartbeatIncrease = edgeIdHeartbeatIncrease.computeIfAbsent(
            time.frontId(),
            __ -> new HeartbeatIncrease()
    );
    heartbeatIncrease.current = Math.max(heartbeatIncrease.current, time.time());
    if (flushCount == 0) {
      IntStream.range(0, ackers.size()).forEach(partition -> {
        long current = partitions.partitionTime(partition, time.frontId().hashCode(), heartbeatIncrease.current);
        if (partitions.partitionTime(partition, time.frontId().hashCode(), heartbeatIncrease.previous) < current) {
          ackers.get(partition).tell(new Heartbeat(new GlobalTime(current, time.frontId())), self());
        }
      });
      heartbeatIncrease.previous = heartbeatIncrease.current;
    }
  }

  private void tick() {
    if (flushCounter == flushCount) {
      flush();
    } else {
      flushCounter++;
    }
  }

  private void handleAck(Ack ack) {
    bumpNodeTimes = true;
    if (flushCount == 0) {
      ackers.get((int) (ack.time().time() % ackers.size())).tell(ack, self());
      return;
    }

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
    if (bumpNodeTimes && ackers.size() > 1) {
      final NodeTime nodeTime = new NodeTime(nodeId, ++this.nodeTime);
      ackers.forEach(acker -> ackerBufferedMessages.get(acker).add(nodeTime));
    }
    bumpNodeTimes = false;

    ackCache.entrySet()
            .stream()
            .map(entry -> new Ack(entry.getKey(), entry.getValue()))
            .collect(Collectors.groupingBy(o -> ackers.get(partitions.timePartition(
                    o.time().frontId().hashCode(),
                    o.time().time()
            ))))
            .forEach((acker, acks) -> acks.forEach(ack -> {
              final List<Object> objects = ackerBufferedMessages.get(acker);
              final Ack random = new Ack(ack.time(), new Random().nextLong());
              for (int i = 0; i < additionalAcks * 2; i++) {
                objects.add(random);
              }
              objects.add(ack);
            }));
    ackCache.clear();

    edgeIdHeartbeatIncrease.forEach((edgeId, heartbeatIncrease) -> {
      IntStream.range(0, ackers.size()).forEach(partition -> {
        long current = partitions.partitionTime(partition, edgeId.hashCode(), heartbeatIncrease.current);
        if (partitions.partitionTime(partition, edgeId.hashCode(), heartbeatIncrease.previous) < current) {
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
      if (!messages.isEmpty()) {
        acker.tell(new BufferedMessages(messages), self());
      }
    });
    flushCounter = 0;
  }

  private enum Flush {
    FLUSH
  }
}
