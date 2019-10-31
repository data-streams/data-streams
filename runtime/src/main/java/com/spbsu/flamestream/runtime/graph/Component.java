package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.HashingVertexStub;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.config.Snapshots;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.api.ComponentPrepared;
import com.spbsu.flamestream.runtime.graph.api.NewRear;
import com.spbsu.flamestream.runtime.graph.state.GroupGroupingState;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Commit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepare;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.HashUnitMap;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Component extends LoggingActor {
  private final String nodeId;
  private final ActorRef localAcker;
  private final ComputationProps props;

  private static class Blocked {
    final Object message;
    final long time;

    Blocked(Object message, long time) {
      this.message = message;
      this.time = time;
    }
  }

  private static class JobaWrapper<WrappedJoba extends Joba> {
    WrappedJoba joba;
    Consumer<DataItem> downstream;
    final Graph.Vertex vertex;
    final Snapshots<Blocked> snapshots;

    JobaWrapper(WrappedJoba joba, Consumer<DataItem> downstream, Graph.Vertex vertex, long minTime) {
      this.joba = joba;
      this.downstream = downstream;
      this.vertex = vertex;
      snapshots = Snapshots.acking ? new Snapshots<>(dataItem -> dataItem.time, minTime, vertex.index()) : null;
    }
  }

  private final Map<GraphManager.Destination, JobaWrapper<?>> wrappedJobas;

  private final Tracing.Tracer groupingSendTracer = Tracing.TRACING.forEvent("fm-send");
  private final Tracing.Tracer acceptInTracer = Tracing.TRACING.forEvent("accept-in", 1000, 1);
  private final Tracing.Tracer acceptOutTracer = Tracing.TRACING.forEvent("accept-in", 1000, 1);
  private final Tracing.Tracer injectInTracer = Tracing.TRACING.forEvent("inject-in");
  private final Tracing.Tracer injectOutTracer = Tracing.TRACING.forEvent("inject-out");

  @Nullable
  private JobaWrapper<SourceJoba> wrappedSourceJoba;
  @Nullable
  private JobaWrapper<SinkJoba> wrappedSinkJoba;

  private Component(String nodeId,
                    Set<Graph.Vertex> componentVertices,
                    Graph graph,
                    HashUnitMap<ActorRef> routes,
                    ActorRef localManager,
                    @Nullable ActorRef localAcker,
                    ComputationProps props,
                    Map<String, GroupGroupingState> stateByVertex) {
    this.nodeId = nodeId;
    this.localAcker = localAcker;
    this.props = props;
    this.wrappedJobas = componentVertices.stream().collect(Collectors.toMap(
            vertex -> GraphManager.Destination.fromVertexId(vertex.id()),
            vertex -> {
              final Joba.Id jobaId = new Joba.Id(nodeId, vertex.id());
              final Joba joba;
              if (vertex instanceof Sink) {
                joba = new SinkJoba(jobaId, context(), props.barrierIsDisabled(), props.getAckerVerticesNumber());
              } else if (vertex instanceof FlameMap) {
                joba = new MapJoba(jobaId, (FlameMap<?, ?>) vertex);
              } else if (vertex instanceof Grouping) {
                if (props.barrierIsDisabled()) {
                  throw new RuntimeException("grouping operations are not supported when barrier is disabled");
                }
                final Grouping grouping = (Grouping) vertex;
                final Collection<HashUnit> values = props.hashGroups()
                        .values()
                        .stream()
                        .flatMap(g -> g.units().stream())
                        .collect(Collectors.toSet());
                stateByVertex.putIfAbsent(vertex.id(), new GroupGroupingState(values));
                joba = new GroupingJoba(jobaId, grouping, stateByVertex.get(vertex.id()));
              } else if (vertex instanceof Source) {
                joba = new SourceJoba(
                        jobaId, props.maxElementsInGraph(), context(), props.barrierIsDisabled(),
                        props.getAckerVerticesNumber()
                );
              } else {
                throw new RuntimeException("Invalid vertex type");
              }
              final List<Consumer<DataItem>> sinks = graph.adjacent(vertex)
                      .map(to -> {
                        final GraphManager.Destination toDest = GraphManager.Destination.fromVertexId(to.id());

                        final Consumer<DataItem> sink;
                        if (componentVertices.contains(to)) {
                          sink = item -> localCall(item, toDest);
                        } else if (to instanceof HashingVertexStub && ((HashingVertexStub) to).hash() != null) {
                          sink = item -> {
                            groupingSendTracer.log(item.xor());
                            final HashFunction hash = ((HashingVertexStub) to).hash();
                            if (hash instanceof HashFunction.Broadcast) {
                              int childId = 0;
                              for (Map.Entry<HashUnit, ActorRef> next : routes.entrySet()) {
                                if (next.getKey().to() == next.getKey().from()) {
                                  //ignore empty ranges
                                  continue;
                                }

                                final DataItem cloned = item.cloneWith(new Meta(
                                        item.meta(),
                                        0,
                                        childId++,
                                        item.meta().globalTime()
                                ));
                                ack(cloned);
                                next.getValue().tell(new AddressedItem(cloned, toDest), self());
                              }
                            } else {
                              ack(item);
                              routes.get(Objects.requireNonNull(hash).applyAsInt(item))
                                      .tell(new AddressedItem(item, toDest), self());
                            }
                          };
                        } else {
                          sink = item -> {
                            ack(item);
                            localManager.tell(new AddressedItem(item, toDest), self());
                          };
                        }
                        return sink;
                      })
                      .collect(Collectors.toList());
              Consumer<DataItem> downstream;
              switch (sinks.size()) {
                case 0:
                  downstream = item -> context().system().deadLetters().tell(item, self());
                  break;
                case 1:
                  //noinspection ConstantConditions
                  downstream = sinks.stream().findAny().get();
                  break;
                default:
                  downstream = item -> {
                    int childId = 0;
                    for (Consumer<DataItem> sink : sinks) {
                      sink.accept(item.cloneWith(new Meta(item.meta(), 0, childId++, item.meta().globalTime())));
                    }
                  };
              }
              if (joba instanceof SinkJoba) {
                return wrappedSinkJoba = new JobaWrapper<>((SinkJoba) joba, downstream, vertex,
                        props.defaultMinimalTime
                );
              } else if (joba instanceof SourceJoba) {
                return wrappedSourceJoba = new JobaWrapper<>((SourceJoba) joba, downstream, vertex,
                        props.defaultMinimalTime
                );
              } else {
                return new JobaWrapper<>(joba, downstream, vertex, props.defaultMinimalTime);
              }
            }
    ));
  }

  public static Props props(String nodeId,
                            Set<Graph.Vertex> componentVertices,
                            Graph graph,
                            HashUnitMap<ActorRef> localManager,
                            ActorRef routes,
                            @Nullable ActorRef localAcker,
                            ComputationProps props,
                            Map<String, GroupGroupingState> stateByVertex) {
    return Props.create(
            Component.class,
            nodeId,
            componentVertices,
            graph,
            localManager,
            routes,
            localAcker,
            props,
            stateByVertex
    ).withDispatcher("processing-dispatcher");
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, this::inject)
            .match(DataItem.class, this::accept)
            .match(MinTimeUpdate.class, this::onMinTime)
            .match(NewRear.class, this::onNewRear)
            .match(Heartbeat.class, h -> {
              if (bufferIfBlocked(h, wrappedSourceJoba, h.time().time() - 1)) {
                return;
              }
              if (localAcker != null) {
                localAcker.forward(h, context());
              }
            })
            .match(UnregisterFront.class, u -> {
              if (localAcker != null) {
                localAcker.forward(u, context());
              }
            })
            .match(Prepare.class, this::onPrepare)
            .match(Commit.class, this::onCommit)
            .build();
  }

  private void onCommit(Commit commit) {
    if (wrappedSourceJoba != null) {
      wrappedSourceJoba.joba.checkpoint(commit.globalTime());
    }
  }

  private void onPrepare(Prepare prepare) {
    wrappedJobas.values().forEach(jobaWrapper -> jobaWrapper.joba.onPrepareCommit(prepare.globalTime()));
    sender().tell(new ComponentPrepared(), self());
  }

  private void inject(AddressedItem addressedItem) {
    final DataItem item = addressedItem.item();
    if (bufferIfBlocked(
            addressedItem,
            wrappedJobas.get(addressedItem.destination()),
            item.meta().globalTime().time()
    )) {
      return;
    }
    injectInTracer.log(item.xor());
    localCall(item, addressedItem.destination());
    ack(item);
    injectOutTracer.log(item.xor());
  }

  private void ack(DataItem dataItem) {
    if (localAcker != null) {
      final GlobalTime globalTime = dataItem.meta().globalTime();
      localAcker.tell(new Ack(globalTime, dataItem.xor()), self());
    }
  }

  private void localCall(DataItem item, GraphManager.Destination destination) {
    accept(item, wrappedJobas.get(destination));
  }

  private void onMinTime(MinTimeUpdate minTime) {
    for (final JobaWrapper<?> jobaWrapper : wrappedJobas.values()) {
      jobaWrapper.joba.onMinTime(minTime.minTime());
      if (jobaWrapper.vertex.index() == minTime.minTime().getVertexIndex()) {
        if (jobaWrapper.snapshots != null) {
          if (!props.hashGroups().get(nodeId).units().stream().allMatch(HashUnit::isEmpty)) {
            jobaWrapper.snapshots.minTimeUpdate(
                    minTime.minTime().time(),
                    scheduleDoneSnapshot(v1 -> receive().apply(v1.message))
            );
          }
        }
      }
    }
  }

  private void accept(DataItem item) {
    if (wrappedSourceJoba != null) {
      if (bufferIfBlocked(item, wrappedSourceJoba, item.meta().globalTime().time())) {
        return;
      }
      acceptInTracer.log(item.xor());
      wrappedSourceJoba.joba.addFront(item.meta().globalTime().frontId(), sender());
      accept(item, wrappedSourceJoba);
      acceptOutTracer.log(item.xor());
    } else {
      throw new IllegalStateException("Source doesn't belong to this component");
    }
  }

  private <WrappedJoba extends Joba> boolean bufferIfBlocked(Object message, JobaWrapper<WrappedJoba> joba, long time) {
    if (joba.snapshots == null) {
      return false;
    }
    return joba.snapshots.putIfBlocked(new Blocked(message, time));
  }

  private void accept(DataItem item, JobaWrapper<?> joba) {
    if (joba.snapshots != null && joba.snapshots.blocked(item.meta().globalTime().time())) {
      throw new RuntimeException("item is blocked: " + item);
    }
    joba.joba.accept(item, joba.downstream, joba.vertex.index(), scheduleDoneSnapshot(joba.downstream));
  }

  @NotNull
  private <Element> Consumer<Supplier<Stream<Element>>> scheduleDoneSnapshot(Consumer<Element> consumer) {
    return onSnapshotDone -> context().system().scheduler().scheduleOnce(
            Duration.ofMillis(Snapshots.durationMs),
            () -> onSnapshotDone.get().forEach(consumer),
            context().dispatcher()
    );
  }

  private void onNewRear(NewRear attachRear) {
    if (wrappedSinkJoba != null) {
      wrappedSinkJoba.joba.attachRear(attachRear.rear());
    } else {
      throw new IllegalStateException("Sink doesn't belong to this component");
    }
  }
}
