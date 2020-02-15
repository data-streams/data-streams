package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.HashingVertexStub;
import com.spbsu.flamestream.core.graph.LabelSpawn;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.core.graph.HashUnit;
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
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Component extends LoggingActor {
  private final ActorRef localAcker;

  private class JobaWrapper<WrappedJoba extends Joba> {
    WrappedJoba joba;
    Consumer<DataItem> downstream;
    final Graph.Vertex vertex;

    JobaWrapper(WrappedJoba joba, Consumer<DataItem> downstream, Graph.Vertex vertex) {
      this.joba = joba;
      this.downstream = downstream;
      this.vertex = vertex;
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
    this.localAcker = localAcker;
    final TrackingComponent sinkTrackingComponent = graph.sinkTrackingComponent();
    this.wrappedJobas = componentVertices.stream().collect(Collectors.toMap(
            vertex -> GraphManager.Destination.fromVertexId(vertex.id()),
            vertex -> {
              final Joba.Id jobaId = new Joba.Id(nodeId, vertex.id());
              final Joba joba;
              if (vertex instanceof Sink) {
                joba = new SinkJoba(jobaId, context(), props.barrierIsDisabled(), sinkTrackingComponent.index);
              } else if (vertex instanceof LabelSpawn) {
                joba = new LabelSpawnJoba(jobaId, (LabelSpawn<?, ?>) vertex);
              } else if (vertex instanceof FlameMap) {
                joba = new MapJoba(jobaId, (FlameMap<?, ?>) vertex);
              } else if (vertex instanceof Grouping) {
                if (props.barrierIsDisabled()) {
                  throw new RuntimeException("grouping operations are not supported when barrier is disabled");
                }
                final Grouping<?> grouping = (Grouping<?>) vertex;
                final Collection<HashUnit> values = props.hashGroups()
                        .values()
                        .stream()
                        .flatMap(g -> g.units().stream())
                        .collect(Collectors.toSet());
                joba = new GroupingJoba(
                        jobaId,
                        grouping,
                        stateByVertex.computeIfAbsent(vertex.id(), __ -> new GroupGroupingState(grouping, values))
                );
              } else if (vertex instanceof Source) {
                joba = new SourceJoba(
                        jobaId,
                        props.maxElementsInGraph(),
                        context(),
                        props.barrierIsDisabled(),
                        sinkTrackingComponent.index
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
                            if (hash == HashFunction.Broadcast.INSTANCE) {
                              int childId = 0;
                              for (Map.Entry<HashUnit, ActorRef> next : routes.entrySet()) {
                                if (next.getKey().isEmpty()) {
                                  //ignore empty ranges
                                  continue;
                                }

                                final DataItem cloned = item.cloneWith(new Meta(
                                        item.meta(),
                                        0,
                                        childId++
                                ));
                                ack(cloned, to);
                                next.getValue().tell(new AddressedItem(cloned, toDest), self());
                              }
                            } else {
                              ack(item, to);
                              routes.get(Objects.requireNonNull(hash).applyAsInt(item))
                                      .tell(new AddressedItem(item, toDest), self());
                            }
                          };
                        } else {
                          sink = item -> {
                            ack(item, to);
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
                      sink.accept(item.cloneWith(new Meta(item.meta(), 0, childId++)));
                    }
                  };
              }
              if (joba instanceof SinkJoba) {
                return wrappedSinkJoba = new JobaWrapper<>((SinkJoba) joba, downstream, vertex);
              } else if (joba instanceof SourceJoba) {
                return wrappedSourceJoba = new JobaWrapper<>((SourceJoba) joba, downstream, vertex);
              } else {
                return new JobaWrapper<>(joba, downstream, vertex);
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
    injectInTracer.log(item.xor());
    localCall(item, addressedItem.destination());
    ack(item, wrappedJobas.get(addressedItem.destination()).vertex);
    injectOutTracer.log(item.xor());
  }

  private void ack(DataItem dataItem, Graph.Vertex to) {
    if (localAcker != null) {
      final GlobalTime globalTime = dataItem.meta().globalTime();
      localAcker.tell(new Ack(
              to.trackingComponent().index,
              new GlobalTime(globalTime.time(), globalTime.frontId()),
              dataItem.xor()
      ), self());
    }
  }

  private void localCall(DataItem item, GraphManager.Destination destination) {
    wrappedJobas.get(destination).joba.accept(item, wrappedJobas.get(destination).downstream);
  }

  private void onMinTime(MinTimeUpdate minTime) {
    wrappedJobas.values().forEach(jobaWrapper -> jobaWrapper.joba.onMinTime(minTime));
  }

  private void accept(DataItem item) {
    if (wrappedSourceJoba != null) {
      acceptInTracer.log(item.xor());
      wrappedSourceJoba.joba.addFront(item.meta().globalTime().frontId(), sender());
      wrappedSourceJoba.joba.accept(item, wrappedSourceJoba.downstream);
      acceptOutTracer.log(item.xor());
    } else {
      throw new IllegalStateException("Source doesn't belong to this component");
    }
  }

  private void onNewRear(NewRear attachRear) {
    if (wrappedSinkJoba != null) {
      wrappedSinkJoba.joba.attachRear(attachRear.rear());
    } else {
      throw new IllegalStateException("Sink doesn't belong to this component");
    }
  }
}
