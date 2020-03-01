package com.spbsu.flamestream.example.labels;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

public abstract class Operator<Type> {
  public final Class<Type> typeClass;
  public final Set<LabelSpawn<?, ?>> labels;

  interface SerializableFunction<T, R> extends Serializable, Function<T, R> {}

  interface SerializableToIntFunction<T> extends Serializable, ToIntFunction<T> {}

  interface SerializableBiFunction<T, U, R> extends Serializable, BiFunction<T, U, R> {}

  private Operator(Class<Type> typeClass, Set<LabelSpawn<?, ?>> labels) {
    this.typeClass = typeClass;
    if (this instanceof LabelSpawn) {
      final HashSet<LabelSpawn<?, ?>> added = new HashSet<>(labels);
      added.add((LabelSpawn<?, ?>) this);
      this.labels = added;
    } else {
      this.labels = labels;
    }
  }

  public <L> LabelSpawn<Type, L> spawnLabel(Class<L> label, SerializableFunction<Type, L> mapper) {
    return new LabelSpawn<>(this, label, mapper);
  }

  public <K> Keyed<Type, K> keyedBy(
          Key<SerializableFunction<Type, K>> key,
          Key<SerializableToIntFunction<K>> hash
  ) {
    return new Keyed<>(this, key, hash);
  }

  public <K> Keyed<Type, K> keyedBy(
          Set<LabelSpawn<?, ?>> keyLabels,
          SerializableFunction<Type, K> keyFunction
  ) {
    return new Keyed<>(
            this,
            new Key<>(keyLabels, keyFunction),
            new Key<>(keyLabels, Objects::hashCode)
    );
  }

  public <K> Keyed<Type, Void> keyedBy(Set<LabelSpawn<?, ?>> keyLabels) {
    return new Keyed<>(this, new Key<>(keyLabels, __ -> null), new Key<>(keyLabels, Objects::hashCode));
  }

  public <K> Keyed<Type, K> keyedBy(SerializableFunction<Type, K> keyFunction) {
    return new Keyed<>(this, new Key<>(Collections.emptySet(), keyFunction), new Key<>(Collections.emptySet(), Objects::hashCode));
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

  public static class Key<K> {
    public final Set<LabelSpawn<?, ?>> labels;
    public final K function;

    public Key(Set<LabelSpawn<?, ?>> labels, K function) {
      this.labels = labels;
      this.function = function;
    }

    public Key(K function) {
      this.labels = Collections.emptySet();
      this.function = function;
    }
  }

  public static final class Keyed<Source, K> {
    public final Operator<Source> source;
    public final Key<SerializableFunction<Source, K>> key;
    public final Key<SerializableToIntFunction<K>> hash;

    public Keyed(
            Operator<Source> source,
            final Key<SerializableFunction<Source, K>> key,
            final Key<SerializableToIntFunction<K>> hash
    ) {
      this.key = key;
      this.hash = hash;
      for (final LabelSpawn<?, ?> label : key.labels) {
        if (!source.labels.contains(label)) {
          throw new IllegalArgumentException(label.toString());
        }
      }
      for (final LabelSpawn<?, ?> label : hash.labels) {
        if (!key.labels.contains(label)) {
          throw new IllegalArgumentException(label.toString());
        }
      }
      this.source = source;
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
    public final Keyed<In, Key> keyed;
    public final SerializableBiFunction<In, S, Tuple2<S, Stream<Out>>> reducer;

    public StatefulMap(
            Keyed<In, Key> keyed,
            Set<LabelSpawn<?, ?>> labels,
            Class<Out> outClass,
            SerializableBiFunction<In, S, Tuple2<S, Stream<Out>>> reducer
    ) {
      super(outClass, labels);
      this.keyed = keyed;
      this.reducer = reducer;
    }
  }

  public static final class LabelMarkers<In, L> extends Operator<L> {
    @org.jetbrains.annotations.NotNull
    public final LabelSpawn<?, L> labelSpawn;
    public final Operator<In> source;

    public LabelMarkers(LabelSpawn<?, L> labelSpawn, Operator<In> source) {
      super(labelSpawn.lClass, Collections.singleton(labelSpawn));
      this.labelSpawn = labelSpawn;
      this.source = source;
    }
  }
}
