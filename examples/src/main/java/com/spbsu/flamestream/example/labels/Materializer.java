package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.core.data.meta.LabelsPresence;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.LabelMarkers;
import com.spbsu.flamestream.core.graph.LabelSpawn;
import com.spbsu.flamestream.core.graph.SerializableBiFunction;
import com.spbsu.flamestream.core.graph.SerializableComparator;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.core.graph.SerializableToIntFunction;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Materializer {
  private final Map<Operator<?>, TrackingComponent> operatorTrackingComponent;
  private final Map<Graph.Vertex, TrackingComponent> vertexTrackingComponent = new HashMap<>();
  private final List<Graph.Vertex> labelSpawns = new ArrayList<>();
  private final Map<Operator.LabelSpawn<?, ?>, List<LabelMarkers>> labelSpawnMarkers = new HashMap<>();

  public static Graph materialize(Flow<?, ?> flow) {
    return new Materializer(flow).graph;
  }

  private final Graph graph;

  private <In, Out> Materializer(Flow<In, Out> flow) {
    final Map<Operator<?>, StronglyConnectedComponent> operatorStronglyConnectedComponent =
            buildStronglyConnectedComponents(flow);
    final Map<StronglyConnectedComponent, TrackingComponent> stronglyConnectedComponentTracking =
            buildTrackingComponents(operatorStronglyConnectedComponent.get(flow.output));
    operatorTrackingComponent = operatorStronglyConnectedComponent.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> stronglyConnectedComponentTracking.get(entry.getValue())
    ));
    final Source source = new Source();
    final Sink sink = new Sink();
    vertexTrackingComponent.put(source, operatorTrackingComponent.get(flow.input));
    vertexTrackingComponent.put(sink, operatorTrackingComponent.get(flow.output));
    graphBuilder
            .link(source, operatorVertex(flow.input))
            .link(operatorVertex(flow.output), sink)
            .vertexTrackingComponent(vertexTrackingComponent::get)
            .init(flow.init);
    graphBuilder.colocate(vertexTrackingComponent.keySet().toArray(Graph.Vertex[]::new));
    graph = graphBuilder.build(source, sink);
  }

  public static class StronglyConnectedComponent {
    final List<Operator<?>> operators = new ArrayList<>();
    final Set<StronglyConnectedComponent> inbound = new HashSet<>();
  }

  private final Graph.Builder graphBuilder = new Graph.Builder();
  private final Map<Operator<?>, Graph.Vertex> cachedOperatorVertex = new HashMap<>();

  Graph.Vertex operatorVertex(Operator<?> operator) {
    {
      final Graph.Vertex vertex = cachedOperatorVertex.get(operator);
      if (vertex != null) {
        return vertex;
      }
    }
    if (operator instanceof Operator.Input) {
      return processInput((Operator.Input<?>) operator);
    } else if (operator instanceof Operator.Map) {
      return processMap((Operator.Map<?, ?>) operator);
    } else if (operator instanceof Operator.StatefulMap) {
      return processStatefulMap((Operator.StatefulMap<?, ?, ?, ?, ?>) operator);
    } else if (operator instanceof Operator.LabelSpawn) {
      return processLabelSpawn((Operator.LabelSpawn<?, ?>) operator);
    } else if (operator instanceof Operator.LabelMarkers) {
      return processLabelMarkers((Operator.LabelMarkers<?, ?>) operator);
    } else {
      throw new IllegalArgumentException(operator.toString());
    }
  }

  <T> Graph.Vertex processInput(Operator.Input<T> input) {
    final FlameMap<T, T> flameMap = new FlameMap.Builder<T, T>(Stream::of, input.typeClass).build();
    cachedOperatorVertex.put(input, flameMap);
    vertexTrackingComponent.put(flameMap, operatorTrackingComponent.get(input));
    for (final Operator<T> source : input.sources) {
      graphBuilder.link(operatorVertex(source), flameMap);
    }
    return flameMap;
  }

  <In, Out> Graph.Vertex processMap(Operator.Map<In, Out> map) {
    final HashFunction hashFunction;
    if (Operator.Broadcast.Instance == map.hash) {
      hashFunction = HashFunction.Broadcast.INSTANCE;
    } else if (map.hash != null) {
      hashFunction = hashFunction(dataItem -> dataItem.payload(map.source.typeClass), map.hash);
    } else {
      hashFunction = null;
    }
    final FlameMap<In, Out> flameMap =
            new FlameMap.Builder<>(map.mapper, map.source.typeClass).hashFunction(hashFunction).build();
    cachedOperatorVertex.put(map, flameMap);
    vertexTrackingComponent.put(flameMap, operatorTrackingComponent.get(map));
    graphBuilder.link(operatorVertex(map.source), flameMap);
    return flameMap;
  }

  private static final class Grouped<Key, Order, Item, State> {
    final Key key;
    final Order order;
    final Item item;
    final State state;
    final boolean isState;

    private Grouped(Key key, Order order, Item item, State state, boolean isState) {
      this.key = key;
      this.order = order;
      this.item = item;
      this.state = state;
      this.isState = isState;
    }
  }

  private static class StatefulMapGroupingEqualz<Key> implements Equalz {
    final LabelsPresence labels;
    final SerializableFunction<DataItem, Key> dataItemKey;

    private StatefulMapGroupingEqualz(LabelsPresence labels, SerializableFunction<DataItem, Key> dataItemKey) {
      this.labels = labels;
      this.dataItemKey = dataItemKey;
    }

    @Override
    public LabelsPresence labels() {
      return labels;
    }

    @Override
    public boolean testPayloads(DataItem o1, DataItem o2) {
      return Objects.equals(dataItemKey.apply(o1), dataItemKey.apply(o2));
    }
  }

  public <K> HashFunction hashFunction(
          SerializableFunction<DataItem, K> function, Operator.Hashing<? super K> key
  ) {
    final LabelsPresence labelsPresence = labelsPresence(key.labels());
    return dataItem -> labelsPresence.hash(key.applyAsInt(function.apply(dataItem)), dataItem.labels());
  }

  <In, Key, O extends Comparable<O>, S, Out> Graph.Vertex processStatefulMap(Operator.StatefulMap<In, Key, O, S, Out> statefulMap) {
    @SuppressWarnings("unchecked") final Class<Tuple2<Grouped<Key, O, In, S>, Stream<Out>>> tupleClass =
            (Class<Tuple2<Grouped<Key, O, In, S>, Stream<Out>>>) (Class<?>) Tuple2.class;
    final Class<In> inClass = statefulMap.keyed.source.typeClass;
    @SuppressWarnings("unchecked") final Class<Grouped<Key, O, In, S>> groupedClass =
            (Class<Grouped<Key, O, In, S>>) (Class<?>) Grouped.class;
    final LabelsPresence keyLabelsPresence = labelsPresence(statefulMap.keyed.key.labels());
    final FlameMap<In, Grouped<Key, O, In, S>> source =
            new FlameMap.Builder<>((In input) -> Stream.of(
                    new Grouped<>(
                            statefulMap.keyed.key.function().apply(input),
                            statefulMap.keyed.order.apply(input),
                            input,
                            (S) null,
                            false
                    )
            ), inClass)
                    .hashFunction(hashFunction(
                            dataItem -> statefulMap.keyed.key.function().apply(dataItem.payload(inClass)),
                            statefulMap.keyed.hash
                    )).build();
    final SerializableFunction<DataItem, Key> dataItemKey = dataItem -> dataItem.payload(groupedClass).key;
    final Grouping<Grouped<Key, O, In, S>> grouping = new Grouping<>(new Grouping.Builder(
            hashFunction(dataItemKey, statefulMap.keyed.hash),
            new StatefulMapGroupingEqualz<>(keyLabelsPresence, dataItemKey),
            2,
            groupedClass
    ).order(SerializableComparator.comparing(dataItem -> dataItem.payload(groupedClass).order)));
    final SerializableBiFunction<S, Grouped<Key, O, In, S>, Tuple2<Grouped<Key, O, In, S>, Stream<Out>>> reduce =
            (state, in) -> {
              final Tuple2<S, Stream<Out>> result = statefulMap.reducer.apply(in.item, state);
              return new Tuple2<>(new Grouped<>(in.key, in.order, in.item, result._1, true), result._2);
            };
    final FlameMap<List<Grouped<Key, O, In, S>>, Tuple2<Grouped<Key, O, In, S>, Stream<Out>>> reducer =
            new FlameMap.Builder<>((List<Grouped<Key, O, In, S>> items) -> {
              switch (items.size()) {
                case 1: {
                  final Grouped<Key, O, In, S> in = items.get(0);
                  if (!in.isState) {
                    return Stream.of(reduce.apply(null, in));
                  }
                  return Stream.empty();
                }
                case 2: {
                  final Grouped<Key, O, In, S> state = items.get(0), in = items.get(1);
                  if (state.isState && !in.isState) {
                    return Stream.of(reduce.apply(state.state, in));
                  }
                  return Stream.empty();
                }
                default:
                  throw new IllegalStateException("Group size should be 1 or 2");
              }
            }, List.class).build();
    final FlameMap<Tuple2<Grouped<Key, O, In, S>, Stream<Out>>, Grouped<Key, O, In, S>> regrouper =
            new FlameMap.Builder<>(
                    (Tuple2<Grouped<Key, O, In, S>, Stream<Out>> item1) -> Stream.of(item1._1),
                    tupleClass
            ).build();
    final FlameMap<Tuple2<Grouped<Key, O, In, S>, Stream<Out>>, Out> sink =
            new FlameMap.Builder<>((Tuple2<Grouped<Key, O, In, S>, Stream<Out>> item) -> item._2, tupleClass).build();
    cachedOperatorVertex.put(statefulMap, sink);
    final TrackingComponent trackingComponent = operatorTrackingComponent.get(statefulMap);
    vertexTrackingComponent.put(source, trackingComponent);
    vertexTrackingComponent.put(grouping, trackingComponent);
    vertexTrackingComponent.put(reducer, trackingComponent);
    vertexTrackingComponent.put(regrouper, trackingComponent);
    vertexTrackingComponent.put(sink, trackingComponent);
    graphBuilder
            .link(operatorVertex(statefulMap.keyed.source), source)
            .link(source, grouping)
            .link(grouping, reducer)
            .link(reducer, regrouper)
            .link(regrouper, grouping)
            .link(reducer, sink);
    return sink;
  }

  @NotNull
  private LabelsPresence labelsPresence(Set<Operator.LabelSpawn<?, ?>> labels) {
    return new LabelsPresence(
            labels.stream().mapToInt(labelSpawn -> labelSpawns.indexOf(operatorVertex(labelSpawn))).toArray()
    );
  }

  <Value, L> Graph.Vertex processLabelSpawn(Operator.LabelSpawn<Value, L> labelSpawn) {
    final List<LabelMarkers> labelMarkers = new ArrayList<>();
    labelSpawnMarkers.put(labelSpawn, labelMarkers);
    final LabelSpawn<Value, L> vertex =
            new LabelSpawn<>(labelSpawn.typeClass, 0, labelSpawn.mapper::apply, labelMarkers);
    labelSpawns.add(vertex);
    cachedOperatorVertex.put(labelSpawn, vertex);
    final TrackingComponent trackingComponent = operatorTrackingComponent.get(labelSpawn);
    vertexTrackingComponent.put(vertex, trackingComponent);
    graphBuilder.link(operatorVertex(labelSpawn.source), vertex);
    return vertex;
  }

  <L> Graph.Vertex processLabelMarkers(Operator.LabelMarkers<?, L> labelMarkers) {
    final LabelMarkers vertex = new LabelMarkers(operatorTrackingComponent.get(labelMarkers));
    operatorVertex(labelMarkers.source);
    labelSpawnMarkers.get(labelMarkers.labelSpawn).add(vertex);
    vertexTrackingComponent.put(vertex, operatorTrackingComponent.get(labelMarkers));
    cachedOperatorVertex.put(labelMarkers, vertex);
    return vertex;
  }

  public static Map<StronglyConnectedComponent, TrackingComponent> buildTrackingComponents(StronglyConnectedComponent sink) {
    final Map<StronglyConnectedComponent, Set<Operator.LabelMarkers<?, ?>>> componentLabelMarkers = new HashMap<>();
    new Consumer<StronglyConnectedComponent>() {
      final Set<StronglyConnectedComponent> visited = new HashSet<>();

      @Override
      public void accept(StronglyConnectedComponent visited) {
        if (this.visited.add(visited)) {
          componentLabelMarkers.put(visited, new HashSet<>());
          visited.inbound.forEach(this);
          for (final Operator<?> operator : visited.operators) {
            if (operator instanceof Operator.LabelMarkers) {
              if (visited.operators.size() > 1) {
                throw new IllegalArgumentException();
              }
              final Operator.LabelMarkers<?, ?> labelMarkers = (Operator.LabelMarkers<?, ?>) operator;
              visited.inbound.forEach(new Consumer<StronglyConnectedComponent>() {
                @Override
                public void accept(StronglyConnectedComponent tracked) {
                  if (componentLabelMarkers.get(tracked).add(labelMarkers)) {
                    tracked.inbound.forEach(this);
                  }
                }
              });
            }
          }
        }
      }
    }.accept(sink);
    final Map<Set<Operator.LabelMarkers<?, ?>>, Set<StronglyConnectedComponent>> trackingComponentsSet =
            componentLabelMarkers.entrySet().stream().collect(Collectors.groupingBy(
                    Map.Entry::getValue,
                    Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
            ));
    final Map<Set<Operator.LabelMarkers<?, ?>>, TrackingComponent> trackingComponents = new HashMap<>();
    final Function<Set<Operator.LabelMarkers<?, ?>>, TrackingComponent> getTrackingComponent =
            new Function<Set<Operator.LabelMarkers<?, ?>>, TrackingComponent>() {
              @Override
              public TrackingComponent apply(Set<Operator.LabelMarkers<?, ?>> labelMarkers) {
                {
                  final TrackingComponent trackingComponent = trackingComponents.get(labelMarkers);
                  if (trackingComponent != null) {
                    return trackingComponent;
                  }
                }
                final Set<TrackingComponent> allInbound = new HashSet<>();
                for (final StronglyConnectedComponent component : trackingComponentsSet.get(labelMarkers)) {
                  for (final StronglyConnectedComponent inbound : component.inbound) {
                    final Set<Operator.LabelMarkers<?, ?>> inboundLabelMarkers = componentLabelMarkers.get(inbound);
                    if (!inboundLabelMarkers.equals(labelMarkers)) {
                      allInbound.add(this.apply(inboundLabelMarkers));
                    }
                  }
                }
                final TrackingComponent trackingComponent = new TrackingComponent(
                        trackingComponents.size(),
                        allInbound
                );
                trackingComponents.put(labelMarkers, trackingComponent);
                return trackingComponent;
              }
            };
    return componentLabelMarkers.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> getTrackingComponent.apply(entry.getValue())
    ));
  }

  public static Map<Operator<?>, StronglyConnectedComponent> buildStronglyConnectedComponents(Flow<?, ?> flow) {
    final Map<Operator<?>, Set<Operator<?>>> transposed = new HashMap<>();
    final List<Operator<?>> exited = new ArrayList<>();
    {
      final Set<Operator<?>> discovered = new HashSet<>();
      new Consumer<Operator<?>>() {
        @Override
        public void accept(Operator<?> operator) {
          if (discovered.add(operator)) {
            transposed.put(operator, new HashSet<>());
            final Collection<? extends Operator<?>> inboundOperators;
            if (operator instanceof Operator.Input) {
              inboundOperators = ((Operator.Input<?>) operator).sources;
            } else if (operator instanceof Operator.Map) {
              inboundOperators = Collections.singleton(((Operator.Map<?, ?>) operator).source);
            } else if (operator instanceof Operator.StatefulMap) {
              inboundOperators = Collections.singleton(((Operator.StatefulMap<?, ?, ?, ?, ?>) operator).keyed.source);
            } else if (operator instanceof Operator.LabelSpawn) {
              inboundOperators = Collections.singleton(((Operator.LabelSpawn<?, ?>) operator).source);
            } else if (operator instanceof Operator.LabelMarkers) {
              inboundOperators = Collections.singleton(((Operator.LabelMarkers<?, ?>) operator).source);
            } else {
              throw new IllegalArgumentException(operator.toString());
            }
            inboundOperators.forEach(this);
            for (final Operator<?> inbound : inboundOperators) {
              transposed.get(inbound).add(operator);
            }
            exited.add(operator);
          }
        }
      }.accept(flow.output);
    }
    final Map<Operator<?>, StronglyConnectedComponent> operatorComponent = new HashMap<>();
    for (int i = exited.size() - 1; i >= 0; i--) {
      final Operator<?> initialOperator = exited.get(i);
      if (!operatorComponent.containsKey(initialOperator)) {
        final StronglyConnectedComponent currentComponent = new StronglyConnectedComponent();
        new Consumer<Operator<?>>() {
          @Override
          public void accept(Operator<?> outboundOperator) {
            final StronglyConnectedComponent connectedComponent = operatorComponent.get(outboundOperator);
            if (connectedComponent != null) {
              if (connectedComponent != currentComponent) {
                connectedComponent.inbound.add(currentComponent);
              }
            } else {
              operatorComponent.put(outboundOperator, currentComponent);
              currentComponent.operators.add(outboundOperator);
              transposed.get(outboundOperator).forEach(this);
            }
          }
        }.accept(initialOperator);
      }
    }
    return operatorComponent;
  }
}
