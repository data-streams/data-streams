package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
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
  public static Graph materialize(Flow<?, ?> flow) {
    return new Materializer(flow).graph;
  }

  private final Graph graph;

  private <In, Out> Materializer(Flow<In, Out> flow) {
    final Source source = new Source();
    final Graph.Vertex
            input = new FlameMap<>(in -> Stream.of(new Record<>(in, Labels.EMPTY)), flow.input.typeClass),
            output = new FlameMap<Record<Out>, Out>(out -> Stream.of(out.value), Record.class);
    final Sink sink = new Sink();
    graphBuilder
            .link(source, input)
            .link(input, operatorVertex(flow.input))
            .link(operatorVertex(flow.output), output)
            .link(output, sink);
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
    final FlameMap<Record<T>, Record<T>> flameMap = new FlameMap<>(Stream::of, Record.class);
    cachedOperatorVertex.put(input, flameMap);
    for (final Operator<T> source : input.sources) {
      graphBuilder.link(operatorVertex(source), flameMap);
    }
    return flameMap;
  }

  <In, Out> Graph.Vertex processMap(Operator.Map<In, Out> map) {
    final FlameMap<Record<? extends In>, Record<? extends Out>> flameMap = new FlameMap<>(record -> {
      final Stream<Out> result = map.mapper.apply(record.value);
      final boolean hasLabels = record.labels.hasAll(map.labels);
      return result.map(out -> {
        if (!hasLabels)
          throw new IllegalArgumentException();
        return new Record<>(out, record.labels);
      });
    }, Record.class);
    cachedOperatorVertex.put(map, flameMap);
    graphBuilder.link(operatorVertex(map.source), flameMap);
    return flameMap;
  }

  private static final class Grouped<Item, State> {
    final Record<Item> item;
    final State state;
    final boolean isState;

    private Grouped(Record<Item> item, State state, boolean isState) {
      this.item = item;
      this.state = state;
      this.isState = isState;
    }
  }

  <In, Key, S, Out> Graph.Vertex processStatefulMap(Operator.StatefulMap<In, Key, S, Out> statefulMap) {
    @SuppressWarnings("unchecked") final Class<Record<In>> inClass = (Class<Record<In>>) (Class<?>) Record.class;
    @SuppressWarnings("unchecked") final Class<Grouped<In, S>> groupedClass =
            (Class<Grouped<In, S>>) (Class<?>) Grouped.class;
    final FlameMap<Record<In>, Grouped<In, S>> source = new FlameMap<>(
            input -> Stream.of(new Grouped<>(input, null, false)),
            inClass
    );
    final Function<Record<In>, Tuple2<Key, Labels>> recordKey = record -> new Tuple2<>(
            statefulMap.source.keyFunction.apply(record.value),
            new Labels(statefulMap.source.keyLabels.stream().map(record.labels::entry).collect(Collectors.toSet()))
    );
    final Grouping<Grouped<In, S>> grouping = new Grouping<>(
            dataItem -> recordKey.apply(dataItem.payload(groupedClass).item).hashCode(),
            (o1, o2) -> recordKey.apply(o1.payload(groupedClass).item)
                    .equals(recordKey.apply(o2.payload(groupedClass).item)),
            2,
            groupedClass
    );
    final BiFunction<S, Record<In>, Record<Tuple2<Grouped<In, S>, Stream<Out>>>> reduce = (state, in) -> {
      final Record<Tuple2<S, Stream<Out>>> result = new Record<>(statefulMap.reducer.apply(in.value, state), in.labels);
      return new Record<>(new Tuple2<>(
              new Grouped<>(in, result.value._1, true),
              result.value._2
      ), result.labels);
    };
    final FlameMap<List<Grouped<In, S>>, Record<Tuple2<Grouped<In, S>, Stream<Out>>>> reducer = new FlameMap<>(
            items -> {
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
            },
            List.class
    );
    final FlameMap<Record<Tuple2<Grouped<In, S>, Stream<Out>>>, Grouped<In, S>> regrouper =
            new FlameMap<>(item -> Stream.of(item.value._1), Record.class);
    final FlameMap<Record<Tuple2<Grouped<In, S>, Stream<Out>>>, Record<Out>> sink =
            new FlameMap<>(item -> item.value._2.map(out -> new Record<>(out, item.labels)), Record.class);
    cachedOperatorVertex.put(statefulMap, sink);
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
    {
      final Graph.Vertex vertex = cachedOperatorVertex.get(labelSpawn);
      if (vertex != null) {
        return vertex;
      }
    }
    final FlameMap<Record<Value>, Record<Value>> flameMap = new FlameMap<>(
            record -> {
              final L label = labelSpawn.mapper.apply(record.value);
              return Stream.of(new Record<>(
                      record.value,
                      record.labels.added(labelSpawn.lClass, () -> label)
              ));
            },
            Record.class
    );
    cachedOperatorVertex.put(labelSpawn, flameMap);
    graphBuilder.link(operatorVertex(labelSpawn.source), flameMap);
    return flameMap;
  }

  <L> Graph.Vertex processLabelMarkers(Operator.LabelMarkers<?, L> labelMarkers) {
    {
      final Graph.Vertex vertex = cachedOperatorVertex.get(labelMarkers);
      if (vertex != null) {
        return vertex;
      }
    }
    final FlameMap<Record<?>, L> flameMap = new FlameMap<>(
            __ -> {throw new RuntimeException();},
            Record.class
    );
    cachedOperatorVertex.put(labelMarkers, flameMap);
    return flameMap;
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
                final Set<TrackingComponent> inbound = new HashSet<>();
                for (final StronglyConnectedComponent component : trackingComponentsSet.get(labelMarkers)) {
                  final Set<Operator.LabelMarkers<?, ?>> inboundLabelMarkers = componentLabelMarkers.get(component);
                  if (!inboundLabelMarkers.equals(labelMarkers)) {
                    inbound.add(this.apply(inboundLabelMarkers));
                  }
                }
                final TrackingComponent trackingComponent = new TrackingComponent(trackingComponents.size(), inbound);
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
