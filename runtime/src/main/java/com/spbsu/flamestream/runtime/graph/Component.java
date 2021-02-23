package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.HashUnit;
import com.spbsu.flamestream.core.graph.HashingVertexStub;
import com.spbsu.flamestream.core.graph.LabelMarkers;
import com.spbsu.flamestream.core.graph.LabelSpawn;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.api.ComponentPrepared;
import com.spbsu.flamestream.runtime.graph.api.NewRear;
import com.spbsu.flamestream.runtime.graph.api.Watermark;
import com.spbsu.flamestream.runtime.graph.state.GroupGroupingState;
import com.spbsu.flamestream.runtime.master.acker.NodeTimes;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Component extends LoggingActor {
  private final String nodeId;
  @NotNull
  private final Graph graph;
  private final HashUnitMap<ActorRef> routes;
  private final ActorRef localManager;
  private final ActorRef localAcker;
  private final ComputationProps props;
  private final TrackingComponent sinkTrackingComponent;

  private class JobaWrapper<WrappedJoba extends Joba> {
    WrappedJoba joba;
    Joba.Sink downstream;
    final Graph.Vertex vertex;
    final Map<Joba.Id, Long> incomingWatermarks = new HashMap<>();
    long outgoingWatermark;

    JobaWrapper(WrappedJoba joba, Joba.Sink downstream, Graph.Vertex vertex) {
      this.joba = joba;
      this.downstream = downstream;
      this.vertex = vertex;
      final var hash = vertex instanceof HashingVertexStub ? ((HashingVertexStub) vertex).hash() : null;
      if (joba instanceof SourceJoba) {
        incomingWatermarks.put(null, props.defaultMinimalTime());
      } else if (hash == null) {
        graph.inverseAdjacent(vertex).forEach(incoming ->
                incomingWatermarks.put(new Joba.Id(nodeId, incoming.id()), props.defaultMinimalTime())
        );
      } else {
        for (final var entry : props.hashGroups().entrySet()) {
          if (!entry.getValue().isEmpty()) {
            graph.inverseAdjacent(vertex).forEach(incoming ->
                    incomingWatermarks.put(new Joba.Id(entry.getKey(), incoming.id()), props.defaultMinimalTime())
            );
          }
        }
      }
    }

    void onWatermark(Joba.Id from, long time) {
      if (incomingWatermarks.put(from, time) == null) {
        throw new IllegalStateException(from.toString());
      }
      final var outgoingWatermark = incomingWatermarks.values().stream().mapToLong(l -> l).min().orElse(Long.MAX_VALUE);
      final var self = new Joba.Id(nodeId, this.vertex.id());
      if (this.outgoingWatermark < outgoingWatermark) {
        graph.adjacent(this.vertex).forEach(to -> {
          final var destination = GraphManager.Destination.fromVertexId(to.id());
          final var hashing = to instanceof HashingVertexStub ? ((HashingVertexStub) to).hash() : null;
          if (hashing == null) {
            wrappedJobas.get(destination).onWatermark(self, outgoingWatermark);
          } else {
            props.hashGroups().forEach((nodeId, hashGroup) -> {
              final var hash = hashGroup.units().stream().flatMapToInt(entry ->
                      entry.isEmpty() ? IntStream.empty() : IntStream.of(entry.from())
              ).findAny();
              if (hash.isPresent()) {
                final var route = routes.get(hash.getAsInt());
                if (route == localManager) {
                  wrappedJobas.get(destination).onWatermark(self, outgoingWatermark);
                } else {
                  route.tell(new Watermark(self, destination, outgoingWatermark), self());
                }
              }
            });
          }
        });
        if (vertex instanceof Sink) {
          onMinTime(new MinTimeUpdate(
                  sinkTrackingComponent.index,
                  new GlobalTime(outgoingWatermark, EdgeId.MAX),
                  new NodeTimes()
          ));
        }
        this.outgoingWatermark = outgoingWatermark;
      }
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

  private Component(
          String nodeId,
          Set<Graph.Vertex> componentVertices,
          Graph graph,
          HashUnitMap<ActorRef> routes,
          ActorRef localManager,
          @Nullable ActorRef localAcker,
          ComputationProps props,
          Map<String, GroupGroupingState> stateByVertex
  ) {
    this.nodeId = nodeId;
    this.graph = graph;
    this.routes = routes;
    this.localManager = localManager;
    this.localAcker = localAcker;
    this.props = props;
    sinkTrackingComponent = graph.sinkTrackingComponent();
    this.wrappedJobas = componentVertices.stream().collect(Collectors.toMap(
            vertex -> GraphManager.Destination.fromVertexId(vertex.id()),
            vertex -> {
              final Joba.Id jobaId = new Joba.Id(nodeId, vertex.id());
              final Function<Graph.Vertex, Joba.Sink> vertexDownstream = to -> {
                final GraphManager.Destination toDest = GraphManager.Destination.fromVertexId(to.id());

                final boolean isLocal = componentVertices.contains(to);
                final HashFunction hash = to instanceof HashingVertexStub ? ((HashingVertexStub) to).hash() : null;
                return new Joba.Sink() {
                  @Override
                  public Runnable schedule(DataItem dataItem) {
                    List<Runnable> list = new ArrayList<>();
                    accept(dataItem, (route, item) -> {
                      Component.this.ack(item, to);
                      list.add(() -> {
                        if (isLocal && route.equals(localManager)) {
                          localCall(item, toDest);
                          Component.this.ack(item, to);
                        } else {
                          route.tell(new AddressedItem(item, toDest), self());
                        }
                      });
                    });
                    return () -> list.forEach(Runnable::run);
                  }

                  @Override
                  public void accept(DataItem dataItem) {
                    accept(dataItem, (route, item) -> {
                      if (isLocal && route.equals(localManager)) {
                        localCall(item, toDest);
                      } else {
                        Component.this.ack(item, to);
                        route.tell(new AddressedItem(item, toDest), self());
                      }
                    });
                  }

                  private void accept(DataItem item, BiConsumer<ActorRef, DataItem> sink) {
                    groupingSendTracer.log(item.xor());
                    if (hash == HashFunction.Broadcast.INSTANCE || hash == null && item.marker()) {
                      int childId = 0;
                      for (final Map.Entry<HashUnit, ActorRef> route : routes.entrySet()) {
                        if (!route.getKey().isEmpty()) {
                          sink.accept(route.getValue(), item.cloneWith(new Meta(
                                  item.meta(),
                                  0,
                                  childId++
                          )));
                        }
                      }
                    } else if (hash == HashFunction.PostBroadcast.INSTANCE || hash == null) {
                      sink.accept(localManager, item);
                    } else {
                      sink.accept(routes.get(Objects.requireNonNull(hash).applyAsInt(item)), item);
                    }
                  }
                };
              };
              final List<Joba.Sink> sinks =
                      graph.adjacent(vertex).map(vertexDownstream).collect(Collectors.toList());
              final Joba.Sink downstream;
              switch (sinks.size()) {
                case 0:
                  downstream = new Joba.Sink() {
                    @Override
                    public Runnable schedule(DataItem dataItem) {
                      context().system().deadLetters().tell(dataItem, self());
                      return () -> {};
                    }

                    @Override
                    public void accept(DataItem dataItem) {
                      context().system().deadLetters().tell(dataItem, self());
                    }
                  };
                  break;
                case 1:
                  downstream = sinks.stream().findAny().get();
                  break;
                default:
                  downstream = new Joba.Sink() {
                    @Override
                    public Runnable schedule(DataItem item) {
                      final List<Runnable> list = new ArrayList<>();
                      int childId = 0;
                      for (Joba.Sink sink : sinks) {
                        list.add(sink.schedule(item.cloneWith(new Meta(item.meta(), 0, childId++))));
                      }
                      return () -> list.forEach(Runnable::run);
                    }

                    @Override
                    public void accept(DataItem item) {
                      int childId = 0;
                      for (Joba.Sink sink : sinks) {
                        sink.accept(item.cloneWith(new Meta(item.meta(), 0, childId++)));
                      }
                    }
                  };
              }
              final Joba joba;
              if (vertex instanceof Sink) {
                joba = new SinkJoba(jobaId, context(), props.barrierIsDisabled(), sinkTrackingComponent.index);
              } else if (vertex instanceof LabelSpawn) {
                final LabelSpawn<?, ?> labelSpawn = (LabelSpawn<?, ?>) vertex;
                joba = new LabelSpawnJoba(
                        jobaId,
                        labelSpawn,
                        labelSpawn.labelMarkers().map(vertexDownstream).collect(Collectors.toList())
                );
              } else if (vertex instanceof LabelMarkers) {
                joba = new LabelMarkersJoba(jobaId, (LabelMarkers) vertex);
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
                        stateByVertex.computeIfAbsent(vertex.id(), __ -> new GroupGroupingState(grouping, values)),
                        sinkTrackingComponent.index
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

  public static Props props(
          String nodeId,
          Set<Graph.Vertex> componentVertices,
          Graph graph,
          HashUnitMap<ActorRef> localManager,
          ActorRef routes,
          @Nullable ActorRef localAcker,
          ComputationProps props,
          Map<String, GroupGroupingState> stateByVertex
  ) {
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
              } else {
                wrappedSourceJoba.onWatermark(null, h.time().time());
              }
            })
            .match(Watermark.class, watermark ->
                    wrappedJobas.get(watermark.to).onWatermark(watermark.from, watermark.time)
            )
            .match(UnregisterFront.class, u -> {
              if (localAcker != null) {
                localAcker.forward(u, context());
              } else {
                wrappedSourceJoba.onWatermark(null, Long.MAX_VALUE);
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
              graph.trackingComponent(to).index,
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
