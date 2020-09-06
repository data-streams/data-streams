package com.spbsu.flamestream.runtime.edge;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.api.Checkpoint;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.api.Start;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.lang.reflect.InvocationTargetException;

public class FrontActor extends LoggingActor {
  private final Front front;
  private final int trackingWindow;

  private FrontActor(
          EdgeContext edgeContext,
          FlameRuntime.FrontInstance<? extends Front> frontInstance,
          int trackingWindow
  ) throws IllegalAccessException, InvocationTargetException, InstantiationException {
    this.trackingWindow = trackingWindow;
    //handle AkkaFront in order to not create yet another actor system
    if (frontInstance.clazz().equals(AkkaFront.class)) {
      front = new AkkaFront(edgeContext, context(), (String) frontInstance.params()[0]);
    } else {
      final Object[] params = new Object[frontInstance.params().length + 1];
      params[0] = edgeContext;
      System.arraycopy(frontInstance.params(), 0, params, 1, frontInstance.params().length);
      front = (Front) frontInstance.clazz().getDeclaredConstructors()[0].newInstance(params);
    }
  }

  public static Props props(EdgeContext edgeContext, FlameRuntime.FrontInstance<?> frontInstance, int trackingWindow) {
    return Props.create(FrontActor.class, edgeContext, frontInstance, trackingWindow);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Start.class, start -> front.onStart(item -> {
              if (item instanceof Heartbeat && Math.floorMod(((Heartbeat) item).time().time(), trackingWindow) != 0) {
                return;
              }
              start.hole().tell(item, self());
            }, start.from()))
            .match(RequestNext.class, request -> front.onRequestNext())
            .match(Checkpoint.class, checkpoint -> front.onCheckpoint(checkpoint.time()))
            .build();
  }
}
