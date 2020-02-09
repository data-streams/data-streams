package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.TrackingComponent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Materializer {
  public static class StronglyConnectedComponent {
    final List<Operator<?>> operators = new ArrayList<>();
    final Set<StronglyConnectedComponent> inbound = new HashSet<>();
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
            } else if (operator instanceof Operator.Reduce) {
              inboundOperators = Collections.singleton(((Operator.Reduce<?, ?, ?, ?>) operator).source.source);
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
