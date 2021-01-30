package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.edge.Front;
import com.spbsu.flamestream.runtime.edge.api.Checkpoint;
import com.spbsu.flamestream.runtime.edge.api.RequestNext;
import com.spbsu.flamestream.runtime.edge.api.Start;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class AkkaFront implements Front {
  private final ActorRef innerActor;

  public AkkaFront(EdgeContext edgeContext, ActorRefFactory refFactory, String localMediatorPath) {
    this.innerActor = refFactory.actorOf(
            RemoteMediator.props(localMediatorPath + "/" + edgeContext.edgeId().nodeId() + "-local"),
            edgeContext.edgeId().nodeId() + "-inner"
    );
  }

  @Override
  public void onStart(Consumer<Object> consumer, GlobalTime from) {
    innerActor.tell(new MediatorStart(consumer, from), ActorRef.noSender());
  }

  @Override
  public void onRequestNext() {
    innerActor.tell(new RequestNext(), ActorRef.noSender());
  }

  @Override
  public void onCheckpoint(GlobalTime to) {
    innerActor.tell(new Checkpoint(to), ActorRef.noSender());
  }

  private static class RemoteMediator extends LoggingActor {
    private final String localMediatorPath;
    private ActorRef localMediator;
    private Consumer<Object> hole = null;

    private RemoteMediator(String localMediatorPath) {
      this.localMediatorPath = localMediatorPath;
    }

    public static Props props(String localMediatorPath) {
      return Props.create(RemoteMediator.class, localMediatorPath);
    }

    @Override
    public void preStart() throws Exception {
      super.preStart();
      final ActorPath path = ActorPaths.fromString(localMediatorPath);
      localMediator = AwaitResolver.resolve(path, context())
              .toCompletableFuture()
              .get();
      log().info("Local mediator has been resolved");
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(MediatorStart.class, s -> {
                hole = s.hole;
                localMediator.tell(new Start(self(), s.from), self());
              })
              .match(DataItem.class, t2 -> hole.accept(t2))
              .match(Heartbeat.class, t1 -> hole.accept(t1))
              .match(UnregisterFront.class, t -> hole.accept(t))
              .match(RequestNext.class, r -> localMediator.tell(r, self()))
              .match(Checkpoint.class, c -> localMediator.tell(c, self()))
              .build();
    }

  }

  public static class MediatorStart {
    final Consumer<Object> hole;
    final GlobalTime from;

    MediatorStart(Consumer<Object> hole, GlobalTime from) {
      this.hole = hole;
      this.from = from;
    }
  }

  public static class LocalMediator extends LoggingActor {
    private final ArrayInvalidatingBucket log = new ArrayInvalidatingBucket();
    private final EdgeContext edgeContext;

    private GlobalTime producerWait = null;
    private GlobalTime lastEmitted = GlobalTime.MIN;
    private long time = 0;

    private int requestDebt;
    private ActorRef sender;
    private ActorRef remoteMediator;
    private boolean unregistered = false;

    private LocalMediator(EdgeContext edgeContext, boolean backPressure) {
      this.edgeContext = edgeContext;
      if (backPressure) {
        requestDebt = 0;
      } else {
        // This can cause overflow, there is a protection below
        requestDebt = Integer.MAX_VALUE;
      }
    }

    public static Props props(EdgeContext context, boolean backPressure) {
      return Props.create(LocalMediator.class, context, backPressure);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(Start.class, start -> {
                onStart(start);
                unstashAll();
                getContext().become(processing());
              })
              .matchAny(o -> stash())
              .build();
    }

    private Receive processing() {
      return ReceiveBuilder.create()
              .match(Start.class, this::onStart)
              .match(RequestNext.class, r -> {
                // Overflow protection
                requestDebt = Math.max(requestDebt + 1, requestDebt);
                tryProcess();
              })
              .match(Checkpoint.class, checkpoint -> log.clearRange(0, log.lowerBound(checkpoint.time())))
              .match(Raw.class, raw -> {
                sender = sender();
                final GlobalTime globalTime = new GlobalTime(time++, edgeContext.edgeId());
                log.insert(new PayloadDataItem(new Meta(globalTime), raw.raw));
                producerWait = globalTime;
                tryProcess();
              })
              .match(
                      Command.class,
                      command -> command == Command.UNREGISTER,
                      command -> {
                        remoteMediator.tell(new UnregisterFront(edgeContext.edgeId()), self());
                        unregistered = true;
                        sender().tell(Command.OK, self());
                      }
              )
              .build();
    }

    private void onStart(Start start) {
      log().info("Got new hole '{}'", start);
      remoteMediator = start.hole();
      if (unregistered) {
        remoteMediator.tell(new UnregisterFront(edgeContext.edgeId()), self());
      }

      time = Math.max(start.from().time(), time);
      lastEmitted = new GlobalTime(start.from().time() - 1, edgeContext.edgeId());
    }

    private void tryProcess() {
      final DataItem entry = log.get(log.lowerBound(lastEmitted));
      if (entry != null && requestDebt > 0) {
        lastEmitted = entry.meta().globalTime();
        requestDebt--;
        remoteMediator.tell(entry, self());
        remoteMediator.tell(new Heartbeat(new GlobalTime(lastEmitted.time() + 1, lastEmitted.frontId())), self());

        if (producerWait != null && lastEmitted.equals(producerWait)) {
          sender.tell(Command.OK, self());
          producerWait = null;
        }
      }
    }
  }

  public static class FrontHandle<T> implements Consumer<T> {
    private final ActorRef localMediator;

    public FrontHandle(ActorRef localMediator) {
      this.localMediator = localMediator;
    }

    @Override
    public void accept(T value) {
      try {
        PatternsCS.ask(localMediator, new Raw(value), FlameConfig.config.bigTimeout()).toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    public void unregister() {
      try {
        PatternsCS.ask(localMediator, Command.UNREGISTER, FlameConfig.config.bigTimeout()).toCompletableFuture().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class Raw {
    private final Object raw;

    private Raw(Object raw) {
      this.raw = raw;
    }
  }

  private enum Command {
    UNREGISTER,
    OK
  }
}