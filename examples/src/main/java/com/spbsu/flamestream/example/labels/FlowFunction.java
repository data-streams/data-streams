package com.spbsu.flamestream.example.labels;

import org.jetbrains.annotations.NotNull;
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
      final Label<L> label = ((Label<L>) record.labels.get(labelTracker.labelMarkers.lClass));
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
                new Labels(Collections.singleton(new Labels.Entry<>(labelMarkers.lClass, label)))
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
    if (operator instanceof Operator.Reduce) {
      return materializeReduce(((Operator.Reduce<?, ?, ?, Output>) operator));
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
        map.map(in).forEach(this::emit);
      }
    };
    materializedOperator.put(map, materialized);
    materialize(map.source).listen(materialized);
    return materialized;
  }

  private <In, Key, S, Out> Materialized<?, Out> materializeReduce(Operator.Reduce<In, Key, S, Out> reduce) {
    final Map<Tuple2<Key, Labels>, S> keyState = new HashMap<>();
    final Function<In, Key> keyFunction =
            reduce.source.keyFunction == null ? ignored -> null : reduce.source.keyFunction;
    final Materialized<In, Out> materialized = new Materialized<In, Out>() {
      @Override
      public void handle(Record<? extends In> in) {
        final Tuple2<Key, Labels> key = new Tuple2<>(keyFunction.apply(in.value), in.labels);
        final Tuple2<S, Stream<Record<Out>>> result = reduce.reduce(in, keyState.get(key));
        keyState.put(key, result._1);
        result._2.forEach(this::emit);
      }
    };
    materializedOperator.put(reduce, materialized);
    materialize(reduce.source.source).listen(materialized);
    return materialized;
  }

  private <Value, L> Materialized<?, Value> materializeLabelSpawn(Operator.LabelSpawn<Value, L> labelSpawn) {
    final Materialized<Value, Value> materialized = new Materialized<Value, Value>() {
      @Override
      public void handle(Record<? extends Value> in) {
        final Label<L> label = new Label<>(labelSpawn.mapper.apply(in.value));
        for (final LabelTracker<?, ?> labelTracker : labelTrackers) {
          if (labelTracker.labelMarkers.lClass.equals(labelSpawn.lClass)) {
            ((LabelTracker<?, L>) labelTracker).spawn(label);
          }
        }
        emit(new Record<>(in.value, in.labels.added(labelSpawn.lClass, label)));
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
      } else if (operator instanceof Operator.Reduce) {
        trackLabels(labelTracker, ((Operator.Reduce<?, ?, ?, Value>) operator).source.source);
      } else if (operator instanceof Operator.LabelSpawn) {
        trackLabels(labelTracker, ((Operator.LabelSpawn<Value, ?>) operator).source);
      } else if (operator instanceof Operator.LabelMarkers) {
        trackLabels(labelTracker, ((Operator.LabelMarkers<?, ?>) operator).source);
      } else {
        throw new IllegalArgumentException(operator.toString());
      }
    }
  }
}
