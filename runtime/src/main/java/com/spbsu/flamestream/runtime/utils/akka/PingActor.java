package com.spbsu.flamestream.runtime.utils.akka;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * User: Artem
 * Date: 26.09.2017
 */
public class PingActor extends LoggingActor {
  private final ActorRef actorToPing;
  private final Object objectForPing;

  private long delayInNanos;
  private boolean started = false;
  private Cancellable scheduler;

  private PingActor(ActorRef actorToPing, Object objectForPing) {
    this.actorToPing = actorToPing;
    this.objectForPing = objectForPing;
  }

  public static Props props(ActorRef actorToPing, Object objectForPing) {
    return Props.create(PingActor.class, actorToPing, objectForPing);
  }

  @Override
  public void postStop() throws Exception {
    stop();
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(Start.class, start -> {
              if (!started) {
                start(start.delayInNanos());
                started = true;
              }
            })
            .match(Stop.class, stop -> {
              if (started) {
                stop();
                started = false;
              }
            })
            .match(InnerPing.class, innerPing -> {
              if (started) {
                handleInnerPing();
              }
            })
            .build();
  }

  private void start(long delayInNanos) {
    this.delayInNanos = delayInNanos;
    if (delayInNanos >= TimeUnit.MILLISECONDS.toNanos(100)) {
      scheduler = context().system().scheduler().schedule(
              Duration.create(0, NANOSECONDS),
              FiniteDuration.apply(delayInNanos, NANOSECONDS),
              actorToPing,
              objectForPing,
              context().system().dispatcher(),
              self()
      );
    } else {
      self().tell(InnerPing.PING, self());
    }
  }

  private void stop() {
    if (scheduler != null) {
      scheduler.cancel();
    }
  }

  private void handleInnerPing() {
    actorToPing.tell(objectForPing, self());
    LockSupport.parkNanos(delayInNanos);
    self().tell(InnerPing.PING, self());
  }

  public static class Start {
    private final long delayInNanos;

    public Start(long delayInNanos) {
      this.delayInNanos = delayInNanos;
    }

    private long delayInNanos() {
      return delayInNanos;
    }
  }

  public static class Stop {
  }

  private enum InnerPing {
    PING
  }
}