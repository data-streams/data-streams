package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.LabelSpawn;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Materializer {
  private final Map<Operator<?>, TrackingComponent> operatorTrackingComponent;
  private final Map<Graph.Vertex, TrackingComponent> vertexTrackingComponent = new HashMap<>();

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
            .link(operatorVertex(flow.output), sink);
    graph = graphBuilder.build(source, sink, vertexTrackingComponent::get);
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
      return processStatefulMap((Operator.StatefulMap<?, ?, ?, ?>) operator);
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
    final FlameMap<In, Out> flameMap = new FlameMap.Builder<>(map.mapper::apply, map.source.typeClass).build();
    cachedOperatorVertex.put(map, flameMap);
    vertexTrackingComponent.put(flameMap, operatorTrackingComponent.get(map));
    graphBuilder.link(operatorVertex(map.source), flameMap);
    return flameMap;
  }

  private static final class Grouped<Item, State> {
    final Item item;
    final State state;
    final boolean isState;

    private Grouped(Item item, State state, boolean isState) {
      this.item = item;
      this.state = state;
      this.isState = isState;
    }
  }

  <In, Key, S, Out> Graph.Vertex processStatefulMap(Operator.StatefulMap<In, Key, S, Out> statefulMap) {
    @SuppressWarnings("unchecked") final Class<In> inClass = statefulMap.source.source.typeClass;
    @SuppressWarnings("unchecked") final Class<Grouped<In, S>> groupedClass =
            (Class<Grouped<In, S>>) (Class<?>) Grouped.class;
    @SuppressWarnings("unchecked") final Class<Tuple2<Grouped<In, S>, Stream<Out>>> tupleClass =
            (Class<Tuple2<Grouped<In, S>, Stream<Out>>>) (Class<?>) Tuple2.class;
    final FlameMap<In, Grouped<In, S>> source = new FlameMap.Builder<>((In input) -> Stream
            .of(new Grouped<>(input, (S) null, false)), inClass).build();
    final Function<DataItem, Tuple2<Key, List<?>>> dataItemKey =
            dataItem -> new Tuple2<>(
                    statefulMap.source.keyFunction.apply(dataItem.payload(groupedClass).item),
                    statefulMap.source.keyLabels
                            .stream()
                            .map(__ -> dataItem.labels().get(0))
                            .collect(Collectors.toList())
            );
    final Grouping<Grouped<In, S>> grouping = new Grouping<>(
            dataItem -> dataItemKey.apply(dataItem).hashCode(),
            (o1, o2) -> dataItemKey.apply(o1).equals(dataItemKey.apply(o2)),
            2,
            groupedClass
    );
    final BiFunction<S, In, Tuple2<Grouped<In, S>, Stream<Out>>> reduce = (state, in) -> {
      final Tuple2<S, Stream<Out>> result = statefulMap.reducer.apply(in, state);
      return new Tuple2<>(new Grouped<>(in, result._1, true), result._2);
    };
    final FlameMap<List<Grouped<In, S>>, Tuple2<Grouped<In, S>, Stream<Out>>> reducer = new FlameMap.Builder<>((List<Grouped<In, S>> items) -> {
      switch (items.size()) {
        case 1: {
          final Grouped<In, S> in = items.get(0);
          if (!in.isState) {
            return Stream.of(reduce.apply(null, in.item));
          }
          return Stream.empty();
        }
        case 2: {
          final Grouped<In, S> state = items.get(0), in = items.get(1);
          if (state.isState && !in.isState) {
            return Stream.of(reduce.apply(state.state, in.item));
          }
          return Stream.empty();
        }
        default:
          throw new IllegalStateException("Group size should be 1 or 2");
      }
    }, List.class).build();
    final FlameMap<Tuple2<Grouped<In, S>, Stream<Out>>, Grouped<In, S>> regrouper =
            new FlameMap.Builder<>(
                    (Tuple2<Grouped<In, S>, Stream<Out>> item1) -> Stream.of(item1._1),
                    tupleClass
            ).build();
    final FlameMap<Tuple2<Grouped<In, S>, Stream<Out>>, Out> sink =
            new FlameMap.Builder<>((Tuple2<Grouped<In, S>, Stream<Out>> item) -> item._2, tupleClass).build();
    cachedOperatorVertex.put(statefulMap, sink);
    final TrackingComponent trackingComponent = operatorTrackingComponent.get(statefulMap);
    vertexTrackingComponent.put(source, trackingComponent);
    vertexTrackingComponent.put(grouping, trackingComponent);
    vertexTrackingComponent.put(reducer, trackingComponent);
    vertexTrackingComponent.put(regrouper, trackingComponent);
    vertexTrackingComponent.put(sink, trackingComponent);
    graphBuilder
            .link(operatorVertex(statefulMap.source.source), source)
            .link(source, grouping)
            .link(grouping, reducer)
            .link(reducer, regrouper)
            .link(regrouper, grouping)
            .link(reducer, sink);
    return sink;
  }

  <Value, L> Graph.Vertex processLabelSpawn(Operator.LabelSpawn<Value, L> labelSpawn) {
    final LabelSpawn<Value, L> vertex = new LabelSpawn<>(labelSpawn.typeClass, 0, labelSpawn.mapper::apply);
    cachedOperatorVertex.put(labelSpawn, vertex);
    final TrackingComponent trackingComponent = operatorTrackingComponent.get(labelSpawn);
    vertexTrackingComponent.put(vertex, trackingComponent);
    graphBuilder.link(operatorVertex(labelSpawn.source), vertex);
    return vertex;
  }

  <L> Graph.Vertex processLabelMarkers(Operator.LabelMarkers<?, L> labelMarkers) {
    final FlameMap<Void, L> vertex = new FlameMap.Builder<Void, L>((Void __) -> {
      throw new RuntimeException();
    }, Void.class).build();
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
                final TrackingComponent trackingComponent = new TrackingComponent(trackingComponents.size(), allInbound);
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
              inboundOperators = Collections.singleton(((Operator.StatefulMap<?, ?, ?, ?>) operator).source.source);
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
