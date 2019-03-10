package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.graph.Joba;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.JobaTime;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.MinTimeUpdateListener;
import com.spbsu.flamestream.runtime.master.acker.api.registry.FrontTicket;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFront;
import com.spbsu.flamestream.runtime.master.acker.api.registry.RegisterFrontFromTime;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.akka.PingActor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class LocalAcker extends LoggingActor {
  private static class NewFrontRegisterer extends LoggingActor {
    static class Registered {
      private final FrontTicket frontTicket;
      private final ActorRef sender;

      Registered(FrontTicket frontTicket, ActorRef sender) {
        this.frontTicket = frontTicket;
        this.sender = sender;
      }

      FrontTicket frontTicket() {
        return this.frontTicket;
      }

      public ActorRef sender() {
        return this.sender;
      }
    }

    public static Props props(ActorRef sender, List<ActorRef> ackers, EdgeId frontId) {
      return Props.create(NewFrontRegisterer.class, sender, ackers, frontId);
    }

    final ActorRef sender;
    final Set<ActorRef> ackersWaitedFor;
    @Nullable
    FrontTicket maxFrontTicket;

    public NewFrontRegisterer(ActorRef sender, List<ActorRef> ackers, EdgeId frontId) {
      this.sender = sender;
      ackersWaitedFor = new LinkedHashSet<>(ackers);
      ackers.forEach(acker -> acker.tell(new RegisterFront(frontId), self()));
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(
                      FrontTicket.class,
                      frontTicket -> {
                        if (maxFrontTicket == null
                                || maxFrontTicket.allowedTimestamp().compareTo(frontTicket.allowedTimestamp()) < 0) {
                          maxFrontTicket = frontTicket;
                        }
                        ackersWaitedFor.remove(sender());
                        if (!ackersWaitedFor.isEmpty()) {
                          return;
                        }
                        context().parent().tell(new Registered(maxFrontTicket, sender), self());
                        context().stop(self());
                      }
              )
              .build();
    }
  }

  private static class AlreadyRegisteredFrontRegisterer extends LoggingActor {
    static class Registered {
      private final FrontTicket frontTicket;
      private final @NotNull
      ActorRef sender;

      Registered(FrontTicket frontTicket, @NotNull ActorRef sender) {
        this.frontTicket = frontTicket;
        this.sender = sender;
      }

      FrontTicket frontTicket() {
        return this.frontTicket;
      }

      @NotNull
      public ActorRef sender() {
        return this.sender;
      }
    }

    public static Props props(@Nullable ActorRef sender, List<ActorRef> ackers, GlobalTime startTime) {
      return Props.create(AlreadyRegisteredFrontRegisterer.class, sender, ackers, startTime);
    }

    final @Nullable
    ActorRef sender;
    final Set<ActorRef> ackersWaitedFor;

    public AlreadyRegisteredFrontRegisterer(@Nullable ActorRef sender, List<ActorRef> ackers, GlobalTime startTime) {
      this.sender = sender;
      ackersWaitedFor = new LinkedHashSet<>(ackers);
      ackers.forEach(acker -> acker.tell(new RegisterFrontFromTime(startTime), self()));
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(
                      FrontTicket.class,
                      frontTicket -> {
                        ackersWaitedFor.remove(sender());
                        if (!ackersWaitedFor.isEmpty()) {
                          return;
                        }
                        if (sender != null) {
                          context().parent().tell(new Registered(frontTicket, sender), self());
                        }
                        context().stop(self());
                      }
              )
              .build();
    }
  }

  private static final int FLUSH_DELAY_IN_MILLIS = 1;
  private static final int FLUSH_COUNT = 100;

  private final HashMap<Joba.Id, Long> jobaTimesCache = new HashMap<>();
  private final SortedMap<GlobalTime, Long> ackCache = new TreeMap<>(Comparator.reverseOrder());
  private final List<Heartbeat> heartbeatCache = new ArrayList<>();
  private final List<UnregisterFront> unregisterCache = new ArrayList<>();

  private final List<ActorRef> ackers;
  private final List<ActorRef> listeners = new ArrayList<>();
  private final MinTimeUpdater minTimeUpdater;
  private final ActorRef pingActor;

  private int flushCounter = 0;

  public LocalAcker(List<ActorRef> ackers) {
    this.ackers = ackers;
    minTimeUpdater = new MinTimeUpdater(ackers);
    pingActor = context().actorOf(PingActor.props(self(), Flush.FLUSH));
  }

  public static Props props(List<ActorRef> ackers) {
    return Props.create(LocalAcker.class, ackers).withDispatcher("processing-dispatcher");
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    ackers.forEach(acker -> acker.tell(new MinTimeUpdateListener(self()), self()));
    pingActor.tell(new PingActor.Start(TimeUnit.MILLISECONDS.toNanos(FLUSH_DELAY_IN_MILLIS)), self());
  }

  @Override
  public void postStop() {
    pingActor.tell(new PingActor.Stop(), self());
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(JobaTime.class, this::handleJobaTime)
            .match(Ack.class, this::handleAck)
            .match(Flush.class, flush -> flush())
            .match(RegisterFront.class, registerFront ->
                    context().actorOf(NewFrontRegisterer.props(sender(), ackers, registerFront.frontId()))
            )
            .match(RegisterFrontFromTime.class, registerFront ->
                    context().actorOf(AlreadyRegisteredFrontRegisterer.props(
                            sender(),
                            ackers,
                            registerFront.startTime
                    ))
            )
            .match(
                    NewFrontRegisterer.Registered.class,
                    registered -> registered.sender.tell(registered.frontTicket(), self())
            )
            .match(
                    AlreadyRegisteredFrontRegisterer.Registered.class,
                    registered -> registered.sender.tell(registered.frontTicket(), self())
            )
            .match(Heartbeat.class, heartbeatCache::add)
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

  private void tick() {
    if (flushCounter == FLUSH_COUNT) {
      flush();
    } else {
      flushCounter++;
    }
  }

  private void handleJobaTime(JobaTime jobaTime) {
    jobaTimesCache.compute(jobaTime.jobaId, (__, value) -> {
      if (value == null) {
        return jobaTime.time;
      } else {
        return Math.max(jobaTime.time, value);
      }
    });

    tick();
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
    jobaTimesCache.forEach((jobaId, value) -> ackers.forEach(acker -> acker.tell(
            new JobaTime(jobaId, value),
            context().parent()
    )));
    jobaTimesCache.clear();

    final boolean acksEmpty = ackCache.isEmpty();
    ackCache.forEach((globalTime, xor) -> ackers.get((int) (globalTime.time() % ackers.size()))
            .tell(new Ack(globalTime, xor), context().parent()));
    ackCache.clear();

    heartbeatCache.forEach(heartbeat -> ackers.forEach(acker -> acker.tell(heartbeat, self())));
    heartbeatCache.clear();

    if (acksEmpty) {
      unregisterCache.forEach(unregisterFront -> ackers.forEach(acker -> acker.tell(unregisterFront, self())));
      unregisterCache.clear();
    }

    flushCounter = 0;
  }

  private enum Flush {
    FLUSH
  }
}
