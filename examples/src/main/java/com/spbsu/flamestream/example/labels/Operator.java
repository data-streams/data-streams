package com.spbsu.flamestream.example.labels;

import org.jetbrains.annotations.Nullable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class Operator<Type> {
  public final Set<Class<?>> labels;

  private Operator(Set<Class<?>> labels) {
    this.labels = labels;
  }

  public <L> Operator<Type> spawnLabel(Class<L> label, Function<Type, L> mapper) {
    return new LabelSpawn<>(this, label, mapper);
  }

  public <Key> KeyedOperator<Type, Key> keyedBy(
          Set<Class<?>> keyLabels,
          @Nullable Function<Type, Key> keyFunction
  ) {
    return new KeyedOperator<>(this, keyLabels, keyFunction);
  }

  public <Key> KeyedOperator<Type, Key> keyedBy(Set<Class<?>> keyLabels) {
    return new KeyedOperator<>(this, keyLabels, null);
  }

  public <Key> KeyedOperator<Type, Key> keyedBy(@Nullable Function<Type, Key> keyFunction) {
    return new KeyedOperator<>(this, Collections.emptySet(), keyFunction);
  }

  public <Output> Operator<Output> map(Function<Type, Output> mapper) {
    return new Map<>(labels, this, in -> Stream.of(new Record<>(mapper.apply(in.value), in.labels)));
  }

  public <Output> Operator<Output> flatMap(Function<Type, Stream<Output>> mapper) {
    return new Map<>(labels, this, in -> mapper.apply(in.value).map(out -> new Record<>(out, in.labels)));
  }

  public Operator<Type> filter(Predicate<Type> predicate) {
    return new Map<>(labels, this, in -> predicate.test(in.value) ? Stream.of(in) : Stream.empty());
  }

  public <L> Operator<L> labelMarkers(Class<L> lClass) {
    return new LabelMarkers<>(lClass, this);
  }

  public static class KeyedOperator<Source, Key> {
    public final Operator<Source> source;
    final Set<Class<?>> keyLabels;
    final @Nullable Function<Source, Key> keyFunction;

    public KeyedOperator(
            Operator<Source> source,
            Set<Class<?>> keyLabels,
            @Nullable Function<Source, Key> keyFunction
    ) {
      for (final Class<?> keyLabel : keyLabels) {
        if (!source.labels.contains(keyLabel)) {
          throw new IllegalArgumentException(keyLabel.toString());
        }
      }
      this.source = source;
      this.keyLabels = keyLabels;
      this.keyFunction = keyFunction;
    }

    public <S, Output> Operator<Output> statefulMap(
            BiFunction<Source, S, Tuple2<S, Output>> mapper
    ) {
      return statefulMap(mapper, source.labels);
    }

    public <S, Output> Operator<Output> statefulMap(
            BiFunction<Source, S, Tuple2<S, Output>> mapper,
            Set<Class<?>> labels
    ) {
      return new Reduce<Source, Key, S, Output>(
              this,
              labels,
              (in, state) -> {
                final Tuple2<S, Output> result = mapper.apply(in.value, state);
                return Tuple2.apply(result._1, Stream.of(new Record<>(result._2, in.labels)));
              }
      );
    }

    public <S, Output> Operator<Output> statefulFlatMap(
            BiFunction<Source, S, Tuple2<S, Stream<Output>>> mapper,
            Set<Class<?>> labels
    ) {
      return new Reduce<Source, Key, S, Output>(
              this,
              labels,
              (in, state) -> {
                final Tuple2<S, Stream<Output>> result = mapper.apply(in.value, state);
                return Tuple2.apply(result._1, result._2.map(out -> new Record<>(out, in.labels)));
              }
      );
    }

    public <S, Output> Operator<Output> statefulMapRecords(
            BiFunction<Record<? extends Source>, S, Tuple2<S, Stream<Record<Output>>>> mapper,
            Set<Class<?>> labels
    ) {
      return new Reduce<>(this, labels, mapper);
    }
  }

  public static class Input<Type> extends Operator<Type> {
    public final List<Operator<Type>> sources = new ArrayList<>();

    public Input() {
      super(Collections.emptySet());
    }

    public Input(Set<Class<?>> labels) {
      super(labels);
    }

    public void link(Operator<Type> operator) {
      for (final Class<?> label : labels) {
        if (!operator.labels.contains(label)) {
          throw new IllegalArgumentException(label.toString());
        }
      }
      sources.add(operator);
    }
  }

  public static class LabelSpawn<Value, L> extends Operator<Value> {
    static Set<Class<?>> addLabel(Set<Class<?>> labels, Class<?> label) {
      final HashSet<Class<?>> added = new HashSet<>(labels);
      added.add(label);
      return added;
    }

    @org.jetbrains.annotations.NotNull public final Operator<Value> source;
    public final Class<L> lClass;
    public final Function<Value, L> mapper;

    public LabelSpawn(Operator<Value> source, Class<L> lClass, Function<Value, L> mapper) {
      super(addLabel(source.labels, lClass));
      this.source = source;
      this.lClass = lClass;
      this.mapper = mapper;
    }
  }

  public static class Map<In, Out> extends Operator<Out> {
    private final Function<Record<? extends In>, Stream<Record<? extends Out>>> mapper;
    public final Operator<In> source;

    public Map(
            Set<Class<?>> labels,
            Operator<In> source,
            Function<Record<? extends In>, Stream<Record<? extends Out>>> mapper
    ) {
      super(labels);
      this.source = source;
      for (final Class<?> label : labels) {
        if (!source.labels.contains(label)) {
          throw new IllegalArgumentException(label.toString());
        }
      }
      this.mapper = mapper;
    }

    Stream<Record<? extends Out>> map(Record<? extends In> in) {
      return mapper.apply(in).peek(out -> {
        for (final Class<?> label : labels) {
          if (!out.labels.get(label).equals(in.labels.get(label))) {
            throw new IllegalArgumentException(label.toString());
          }
        }
      });
    }
  }

  public static class Reduce<In, Key, S, Out> extends Operator<Out> {
    public final KeyedOperator<In, Key> source;
    private final BiFunction<Record<? extends In>, S, Tuple2<S, Stream<Record<Out>>>> reducer;

    public Reduce(
            KeyedOperator<In, Key> source,
            Set<Class<?>> labels,
            BiFunction<Record<? extends In>, S, Tuple2<S, Stream<Record<Out>>>> reducer
    ) {
      super(labels);
      this.reducer = reducer;
      this.source = source;
    }

    Tuple2<S, Stream<Record<Out>>> reduce(Record<? extends In> in, S state) {
      final Tuple2<S, Stream<Record<Out>>> result = reducer.apply(in, state);
      return Tuple2.apply(result._1, result._2.peek(out -> {
        for (final Class<?> label : labels) {
          if (!out.labels.get(label).equals(in.labels.get(label))) {
            throw new IllegalArgumentException(label.toString());
          }
        }
      }));
    }
  }

  public static class LabelMarkers<In, L> extends Operator<L> {
    public final Class<L> lClass;
    public final Operator<In> source;

    public LabelMarkers(Class<L> lClass, Operator<In> source) {
      super(Collections.singleton(lClass));
      this.lClass = lClass;
      this.source = source;
    }
  }
}
