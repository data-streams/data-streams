package com.spbsu.flamestream.example.labels;

import org.jetbrains.annotations.Nullable;
import scala.Tuple2;

import java.io.Serializable;
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
  public final Class<Type> typeClass;
  public final Set<LabelSpawn<?, ?>> labels;

  interface SerializableFunction<T, R> extends Serializable, Function<T, R> {}

  interface SerializableBiFunction<T, U, R> extends Serializable, BiFunction<T, U, R> {}

  static Set<LabelSpawn<?, ?>> addLabel(Set<LabelSpawn<?, ?>> labels, LabelSpawn<?, ?> label) {
    final HashSet<LabelSpawn<?, ?>> added = new HashSet<>(labels);
    added.add(label);
    return added;
  }

  private Operator(Class<Type> typeClass, Set<LabelSpawn<?, ?>> labels) {
    this.typeClass = typeClass;
    this.labels = this instanceof LabelSpawn ? addLabel(labels, (LabelSpawn<?, ?>) this) : labels;
  }

  public <L> LabelSpawn<Type, L> spawnLabel(Class<L> label, SerializableFunction<Type, L> mapper) {
    return new LabelSpawn<>(this, label, mapper);
  }

  public <Key> KeyedOperator<Type, Key> keyedBy(
          Set<LabelSpawn<?, ?>> keyLabels,
          SerializableFunction<Type, Key> keyFunction
  ) {
    return new KeyedOperator<>(this, keyLabels, keyFunction);
  }

  public <Key> KeyedOperator<Type, Void> keyedBy(Set<LabelSpawn<?, ?>> keyLabels) {
    return new KeyedOperator<>(this, keyLabels, __ -> null);
  }

  public <Key> KeyedOperator<Type, Key> keyedBy(SerializableFunction<Type, Key> keyFunction) {
    return new KeyedOperator<>(this, Collections.emptySet(), keyFunction);
  }

  public <Output> Operator<Output> map(Class<Output> outputClass, SerializableFunction<Type, Output> mapper) {
    return new Map<>(labels, this, outputClass, in -> Stream.of(mapper.apply(in)));
  }

  public <Output> Operator<Output> flatMap(
          Class<Output> outputClass,
          SerializableFunction<Type, Stream<Output>> mapper
  ) {
    return new Map<>(labels, this, outputClass, mapper);
  }

  public Operator<Type> filter(Predicate<Type> predicate) {
    return new Map<>(labels, this, this.typeClass, in -> predicate.test(in) ? Stream.of(in) : Stream.empty());
  }

  public <L> Operator<L> labelMarkers(LabelSpawn<?, L> lClass) {
    return new LabelMarkers<>(lClass, this);
  }

  public static final class KeyedOperator<Source, Key> {
    public final Operator<Source> source;
    final Set<LabelSpawn<?, ?>> keyLabels;
    final Function<Source, Key> keyFunction;

    public KeyedOperator(
            Operator<Source> source,
            Set<LabelSpawn<?, ?>> keyLabels,
            SerializableFunction<Source, Key> keyFunction
    ) {
      for (final LabelSpawn<?, ?> keyLabel : keyLabels) {
        if (!source.labels.contains(keyLabel)) {
          throw new IllegalArgumentException(keyLabel.toString());
        }
      }
      this.source = source;
      this.keyLabels = keyLabels;
      this.keyFunction = keyFunction;
    }

    public <S, Output> Operator<Output> statefulMap(
            Class<Output> outputClass, SerializableBiFunction<Source, S, Tuple2<S, Output>> mapper
    ) {
      return statefulMap(outputClass, mapper, source.labels);
    }

    public <S, Output> Operator<Output> statefulMap(
            Class<Output> outputClass,
            SerializableBiFunction<Source, S, Tuple2<S, Output>> mapper,
            Set<LabelSpawn<?, ?>> labels
    ) {
      return new StatefulMap<>(this, labels, outputClass, (Source in, S state) -> {
        final Tuple2<S, Output> out = mapper.apply(in, state);
        return new Tuple2<>(out._1, Stream.of(out._2));
      });
    }

    public <S, Output> Operator<Output> statefulFlatMap(
            Class<Output> outputClass,
            SerializableBiFunction<Source, S, Tuple2<S, Stream<Output>>> mapper,
            Set<LabelSpawn<?, ?>> labels
    ) {
      return new StatefulMap<>(this, labels, outputClass, mapper);
    }
  }

  public static final class Input<Type> extends Operator<Type> {
    public final List<Operator<Type>> sources = new ArrayList<>();

    public Input(Class<Type> typeClass) {
      super(typeClass, Collections.emptySet());
    }

    public Input(Class<Type> typeClass, Set<LabelSpawn<?, ?>> labels) {
      super(typeClass, labels);
    }

    public void link(Operator<Type> operator) {
      for (final LabelSpawn<?, ?> label : labels) {
        if (!operator.labels.contains(label)) {
          throw new IllegalArgumentException(label.toString());
        }
      }
      sources.add(operator);
    }
  }

  public static final class LabelSpawn<Value, L> extends Operator<Value> {
    @org.jetbrains.annotations.NotNull
    public final Operator<Value> source;
    public final Class<L> lClass;
    public final SerializableFunction<Value, L> mapper;

    public LabelSpawn(Operator<Value> source, Class<L> lClass, SerializableFunction<Value, L> mapper) {
      super(source.typeClass, source.labels);
      this.source = source;
      this.lClass = lClass;
      this.mapper = mapper;
    }
  }

  public static final class Map<In, Out> extends Operator<Out> {
    public final SerializableFunction<In, Stream<Out>> mapper;
    public final Operator<In> source;

    public Map(
            Set<LabelSpawn<?, ?>> labels,
            Operator<In> source,
            Class<Out> outClass,
            SerializableFunction<In, Stream<Out>> mapper
    ) {
      super(outClass, labels);
      this.source = source;
      this.mapper = mapper;
    }
  }

  public static final class StatefulMap<In, Key, S, Out> extends Operator<Out> {
    public final KeyedOperator<In, Key> source;
    public final SerializableBiFunction<In, S, Tuple2<S, Stream<Out>>> reducer;

    public StatefulMap(
            KeyedOperator<In, Key> source,
            Set<LabelSpawn<?, ?>> labels,
            Class<Out> outClass,
            SerializableBiFunction<In, S, Tuple2<S, Stream<Out>>> reducer
    ) {
      super(outClass, labels);
      this.reducer = reducer;
      this.source = source;
    }
  }

  public static final class LabelMarkers<In, L> extends Operator<L> {
    public final Class<L> lClass;
    @org.jetbrains.annotations.NotNull
    public final LabelSpawn<?, L> labelSpawn;
    public final Operator<In> source;

    public LabelMarkers(LabelSpawn<?, L> labelSpawn, Operator<In> source) {
      super(labelSpawn.lClass, Collections.singleton(labelSpawn));
      this.lClass = labelSpawn.lClass;
      this.labelSpawn = labelSpawn;
      this.source = source;
    }
  }
}
