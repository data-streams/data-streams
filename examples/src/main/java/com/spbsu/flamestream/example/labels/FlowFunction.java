package com.spbsu.flamestream.example.labels;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FlowFunction<FlowInput, FlowOutput> {
  private final Map<Operator<?>, Materialized<?, ?>> materializedOperator = new HashMap<>();
  private final Materialized<FlowInput, ?> consumer;

  public static class Label<Value> implements com.spbsu.flamestream.example.labels.Label<Value> {
    private final Value value;

    public Label(Value value) {this.value = value;}

    @Override
    public Value value() {
      return value;
    }
  }

  private static abstract class Materialized<Input, Output> implements Consumer<Record<? extends Input>> {
    final List<Consumer<Record<? extends Output>>> listeners = new ArrayList<>();
    final Set<LabelTracker<?, ?>> labelTrackers = new HashSet<>();

    @Override
    public void accept(Record<? extends Input> record) {
      handle(record);
      changeLabelCounts(record, -1, this);
    }

    private void changeLabelCounts(Record<?> record, int diff, Materialized<?, ?> materialized) {
      for (final LabelTracker<?, ?> labelTracker : labelTrackers) {
        if (materialized.labelTrackers.contains(labelTracker)) {
          changeLabelTrackerCount(record, diff, labelTracker);
        }
      }
    }

    private <L> void changeLabelTrackerCount(Record<?> record, int diff, LabelTracker<?, L> labelTracker) {
      final Label<L> label = record.labels.get(labelTracker.labelMarkers.labelSpawn);
      if (label != null)
        labelTracker.change(label, diff);
    }

    abstract void handle(Record<? extends Input> record);

    protected void emit(Record<? extends Output> o) {
      for (final Consumer<Record<? extends Output>> listener : listeners) {
        if (listener instanceof Materialized) {
          changeLabelCounts(o, 1, (Materialized<Output, ?>) listener);
        }
        listener.accept(o);
      }
    }

    public void listen(Consumer<Record<? extends Output>> consumer) {
      listeners.add(consumer);
    }
  }

  private static class LabelTracker<In, L> {
    final Operator.LabelMarkers<In, L> labelMarkers;
    final Map<Label<? extends L>, Integer> labelCount = new HashMap<>();
    final Materialized<In, L> materialized = new Materialized<In, L>() {
      @Override
      public void handle(Record<? extends In> in) {
        throw new RuntimeException();
      }
    };

    LabelTracker(Operator.LabelMarkers<In, L> labelMarkers) {
      this.labelMarkers = labelMarkers;
    }

    void spawn(Label<L> label) {
      labelCount.put(label, 0);
    }

    void change(Label<? extends L> label, int diff) {
      if (labelCount.compute(label, (ignored, value) -> value + diff == 0 ? null : value + diff) == null) {
        materialized.emit(new Record<>(
                label.value,
                new Labels(Collections.singleton(new Labels.Entry<>(labelMarkers.labelSpawn, label)))
        ));
      }
    }
  }

  public FlowFunction(Flow<FlowInput, FlowOutput> flow, Consumer<FlowOutput> consumer) {
    materialize(flow.output).listen(out -> consumer.accept(out.value));
    this.consumer = (Materialized<FlowInput, ?>) materializedOperator.get(flow.input);
  }

  private <Output> Materialized<?, Output> materialize(Operator<Output> operator) {
    {
      final Materialized<?, ?> materialized = materializedOperator.get(operator);
      if (materialized != null) {
        return (Materialized<?, Output>) materialized;
      }
    }
    if (operator instanceof Operator.Input) {
      return materializeInput((Operator.Input<Output>) operator);
    }
    if (operator instanceof Operator.Map) {
      return materializeMap((Operator.Map<?, Output>) operator);
    }
    if (operator instanceof Operator.StatefulMap) {
      return materializeReduce(((Operator.StatefulMap<?, ?, ?, Output>) operator));
    }
    if (operator instanceof Operator.LabelSpawn) {
      return materializeLabelSpawn(((Operator.LabelSpawn<Output, ?>) operator));
    }
    if (operator instanceof Operator.LabelMarkers) {
      return (Materialized<?, Output>) materializeLabelMarkers((Operator.LabelMarkers<?, ?>) operator);
    }
    throw new RuntimeException();
  }

  @NotNull
  private <Output> Materialized<Output, Output> materializeInput(Operator.Input<Output> input) {
    final Materialized<Output, Output> materialized = new Materialized<Output, Output>() {
      @Override
      public void handle(Record<? extends Output> o) {
        emit(o);
      }
    };
    materializedOperator.put(input, materialized);
    for (final Operator<Output> source : input.sources) {
      materialize(source).listen(materialized);
    }
    return materialized;
  }

  private <In, Out> Materialized<?, Out> materializeMap(Operator.Map<In, Out> map) {
    final Materialized<In, Out> materialized = new Materialized<In, Out>() {
      @Override
      public void handle(Record<? extends In> in) {
        final Stream<Out> result = map.mapper.apply(in.value);
        final boolean hasLabels = in.labels.hasAll(map.labels);
        result.map(out -> {
          if (!hasLabels)
            throw new IllegalArgumentException();
          return new Record<>(out, in.labels);
        }).forEach(this::emit);
      }
    };
    materializedOperator.put(map, materialized);
    materialize(map.source).listen(materialized);
    return materialized;
  }

  private <In, Key, S, Out> Materialized<?, Out> materializeReduce(Operator.StatefulMap<In, Key, S, Out> statefulMap) {
    final Map<Tuple2<Key, Labels>, S> keyState = new HashMap<>();
    final Function<In, Key> keyFunction =
            statefulMap.source.keyFunction == null ? ignored -> null : statefulMap.source.keyFunction;
    final Materialized<In, Out> materialized = new Materialized<In, Out>() {
      @Override
      public void handle(Record<? extends In> in) {
        final Tuple2<Key, Labels> key = new Tuple2<>(keyFunction.apply(in.value), in.labels);
        final Record<Tuple2<S, Stream<Out>>> result = new Record<>(statefulMap.reducer.apply(
                in.value,
                keyState.get(key)
        ), in.labels);
        keyState.put(key, result.value._1);
        result.value._2.forEach(out -> emit(new Record<>(out, result.labels)));
      }
    };
    materializedOperator.put(statefulMap, materialized);
    materialize(statefulMap.source.source).listen(materialized);
    return materialized;
  }

  private <Value, L> Materialized<?, Value> materializeLabelSpawn(Operator.LabelSpawn<Value, L> labelSpawn) {
    final Materialized<Value, Value> materialized = new Materialized<Value, Value>() {
      @Override
      public void handle(Record<? extends Value> in) {
        final Label<L> label = new Label<>(labelSpawn.mapper.apply(in.value));
        for (final LabelTracker<?, ?> labelTracker : labelTrackers) {
          if (labelTracker.labelMarkers.labelSpawn.equals(labelSpawn)) {
            ((LabelTracker<?, L>) labelTracker).spawn(label);
          }
        }
        emit(new Record<>(in.value, in.labels.added(labelSpawn, label)));
      }
    };
    materializedOperator.put(labelSpawn, materialized);
    materialize(labelSpawn.source).listen(materialized);
    return materialized;
  }

  private <In, L> Materialized<?, L> materializeLabelMarkers(Operator.LabelMarkers<In, L> labelMarkers) {
    final LabelTracker<In, L> labelTracker = new LabelTracker<>(labelMarkers);
    materializedOperator.put(labelMarkers, labelTracker.materialized);
    materialize(labelMarkers.source);
    trackLabels(labelTracker, labelMarkers.source);
    return labelTracker.materialized;
  }

  void put(FlowInput input) {
    consumer.accept(new Record<>(input, new Labels(Collections.emptySet())));
  }

  private <L, Value> void trackLabels(LabelTracker<?, L> labelTracker, Operator<Value> operator) {
    if (operator.equals(labelTracker.labelMarkers)) {
      throw new RuntimeException();
    }
    final Materialized<?, Value> materialized = materialize(operator);
    if (materialized.labelTrackers.add(labelTracker)) {
      if (operator instanceof Operator.Input) {
        for (final Operator<Value> source : ((Operator.Input<Value>) operator).sources) {
          trackLabels(labelTracker, source);
        }
      } else if (operator instanceof Operator.Map) {
        trackLabels(labelTracker, ((Operator.Map<?, Value>) operator).source);
      } else if (operator instanceof Operator.StatefulMap) {
        trackLabels(labelTracker, ((Operator.StatefulMap<?, ?, ?, Value>) operator).source.source);
      } else if (operator instanceof Operator.LabelSpawn) {
        trackLabels(labelTracker, ((Operator.LabelSpawn<Value, ?>) operator).source);
      } else if (operator instanceof Operator.LabelMarkers) {
        trackLabels(labelTracker, ((Operator.LabelMarkers<?, ?>) operator).source);
      } else {
        throw new IllegalArgumentException(operator.toString());
      }
    }
  }

  private static class Record<Value> {
    final Value value;
    final Labels labels;
  
    public Record(Value value, Labels labels) {
      this.value = value;
      this.labels = labels;
    }
  }

  private static class Labels {
    private final Map<Operator.LabelSpawn<?, ?>, Label<?>> all;
  
    public static class Entry<Value> {
      public final Operator.LabelSpawn<?, Value> aClass;
      public final Label<? extends Value> value;
  
      public Entry(Operator.LabelSpawn<?, Value> aClass, Label<? extends Value> value) {
        this.aClass = aClass;
        this.value = value;
      }
  
      @Override
      public boolean equals(Object obj) {
        if (obj == null) {
          return false;
        }
        return obj.equals(aClass);
      }
  
      @Override
      public int hashCode() {
        return aClass.hashCode();
      }
    }
  
    public static final Labels EMPTY = new Labels(Collections.emptySet());
  
    public Labels(Set<Entry<?>> entries) {
      all = entries.stream().collect(Collectors.toMap(entry -> entry.aClass, entry -> entry.value));
    }
  
    private Labels(Map<Operator.LabelSpawn<?, ?>, Label<?>> all) {
      this.all = all;
    }
  
    @Override
    public int hashCode() {
      return all.hashCode();
    }
  
    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      return obj.equals(all);
    }
  
    public <Value> Labels added(Operator.LabelSpawn<?, Value> valueClass, Label<Value> value) {
      final HashMap<Operator.LabelSpawn<?, ?>, Label<?>> added = new HashMap<>(all);
      added.put(valueClass, value);
      return new Labels(added);
    }
  
    @Nullable
    <Value> Label<Value> get(Operator.LabelSpawn<?, Value> aClass) {
      return (Label<Value>) all.get(aClass);
    }
  
    @Nullable <Value> Entry<Value> entry(Operator.LabelSpawn<?, Value> aClass) {
      return new Entry<>(aClass, (Label<Value>) all.get(aClass));
    }
  
    public boolean hasAll(Set<Operator.LabelSpawn<?, ?>> classes) {
      for (final Operator.LabelSpawn<?, ?> aClass : classes) {
        if (!all.containsKey(aClass))
          return false;
      }
      return true;
    }
  }
}
