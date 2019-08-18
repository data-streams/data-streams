package com.spbsu.flamestream.runtime.utils.akka;

import akka.actor.AbstractActorWithStash;
import akka.actor.SupervisorStrategy;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.Optional;

public abstract class LoggingActor extends AbstractActorWithStash {
  private final LoggingAdapter log = Logging.getLogger(context().system(), self());

  @Override
  public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
    receive.apply(msg);
  }

  @Override
  public void preStart() throws Exception {
    log().info("Starting...");
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    log().info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
    log().error("Restarting, reason: {}, payload: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void unhandled(Object message) {
    log().error("Can't handle payload: {}", message);
    super.unhandled(message);
  }

  @Override
  public SupervisorStrategy supervisorStrategy() {
    final SupervisorStrategy supervisorStrategy = super.supervisorStrategy();
    return SupervisorStrategy.stoppingStrategy();
  }

  protected final LoggingAdapter log() {
    return log;
  }
}
