package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.graph.SerializableBiFunction;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.core.graph.SerializablePredicate;
import com.spbsu.flamestream.core.graph.SerializableToIntFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class Operator<Type> {
  public final Class<Type> typeClass;
  public final Set<LabelSpawn<?, ?>> labels;

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

  public class MapBuilder<Output> {
    final Class<Output> outputClass;
    final SerializableFunction<Type, Stream<Output>> mapper;
    Hashing<? super Type> hash;

    public MapBuilder(Class<Output> outputClass, SerializableFunction<Type, Stream<Output>> mapper) {
      this.outputClass = outputClass;
      this.mapper = mapper;
    }

    public MapBuilder<Output> hash(Hashing<? super Type> hash) {
      this.hash = hash;
      return this;
    }

    public Map<Type, Output> build() {
      return new Map<>(Operator.this, this);
    }
  }

  enum DefaultOrder {
    Instance
  }

  public KeyedBuilder<Void, DefaultOrder> newKeyedBuilder() {
    return new KeyedBuilder<>(__ -> null, __ -> DefaultOrder.Instance);
  }

  public <K> KeyedBuilder<K, DefaultOrder> newKeyedBuilder(SerializableFunction<Type, K> key) {
    return new KeyedBuilder<>(key, __ -> DefaultOrder.Instance);
  }

  public <K, O extends Comparable<O>> KeyedBuilder<K, O> newKeyedBuilder(
          SerializableFunction<Type, K> key, SerializableFunction<Type, O> order
  ) {
    return new KeyedBuilder<>(key, order);
  }

  public class KeyedBuilder<K, O extends Comparable<O>> {
    final SerializableFunction<Type, K> keyFunction;
    final Function<Type, O> order;
    private Set<LabelSpawn<?, ?>> keyLabels = Collections.emptySet();
    private Hashing<? super K> hash = Object::hashCode;
    private boolean orderedByProcessingTime = false;

    private KeyedBuilder(SerializableFunction<Type, K> key, SerializableFunction<Type, O> order) {
      this.keyFunction = key;
      this.order = order;
    }

    public KeyedBuilder<K, O> hash(Hashing<? super K> hash) {
      this.hash = hash;
      return this;
    }

    public KeyedBuilder<K, O> orderedByProcessingTime(boolean orderedByProcessingTime) {
      this.orderedByProcessingTime = orderedByProcessingTime;
      return this;
    }

    public Operator<Type> operator() {
      return Operator.this;
    }

    public KeyedBuilder<K, O> keyLabels(Set<LabelSpawn<?, ?>> keyLabels) {
      this.keyLabels = keyLabels;
      return this;
    }

    Keyed<Type, K, O> build() {
      return new Keyed<>(this);
    }
  }

  public <Output> Operator<Output> map(Class<Output> outputClass, SerializableFunction<Type, Output> mapper) {
    return new MapBuilder<>(outputClass, in -> Stream.of(mapper.apply(in))).build();
  }

  public <Output> Operator<Output> flatMap(
          Class<Output> outputClass,
          SerializableFunction<Type, Stream<Output>> mapper
  ) {
    return new MapBuilder<>(outputClass, mapper).build();
  }

  public Operator<Type> filter(SerializablePredicate<Type> predicate) {
    return new Map<>(labels, this, this.typeClass, in -> predicate.test(in) ? Stream.of(in) : Stream.empty());
  }

  public <L> Operator<L> labelMarkers(LabelSpawn<?, L> lClass) {
    return new LabelMarkers<>(lClass, this, null);
  }

  public <L> Operator<L> labelMarkers(LabelSpawn<?, L> lClass, Hashing<? super L> hashing) {
    return new LabelMarkers<>(lClass, this, hashing);
  }

  public interface Hashing<T> extends SerializableToIntFunction<T> {
    default Set<LabelSpawn<?, ?>> labels() {
      return Collections.emptySet();
    }

    enum Special implements Hashing<Object> {
      Broadcast, PostBroadcast;

      @Override
      public int applyAsInt(Object o) {
        throw new UnsupportedOperationException();
      }
    }
  }

  public interface Key<F> {
    F function();

    default Set<LabelSpawn<?, ?>> labels() {
      return Collections.emptySet();
    }
  }

  public static final class Keyed<Source, K, O extends Comparable<O>> {
    public final Operator<Source> source;
    public final Function<Source, O> order;
    public final Key<SerializableFunction<Source, K>> key;
    public final Hashing<? super K> hash;
    public final boolean orderedByProcessingTime;

    public Keyed(Operator<Source>.KeyedBuilder<K, O> builder) {
      source = builder.operator();
      order = builder.order;
      key = new Key<SerializableFunction<Source, K>>() {
        @Override
        public Set<LabelSpawn<?, ?>> labels() {
          return builder.keyLabels;
        }

        @Override
        public SerializableFunction<Source, K> function() {
          return builder.keyFunction;
        }
      };
      hash = builder.hash;
      orderedByProcessingTime = builder.orderedByProcessingTime;
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
            SerializableBiFunction<Source, S, Tuple2<S, Stream<Output>>> mapper
    ) {
      return new StatefulMap<>(this, Collections.emptySet(), outputClass, mapper);
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
    public final List<Operator<? extends Type>> sources = new ArrayList<>();

    public Input(Class<Type> typeClass) {
      super(typeClass, Collections.emptySet());
    }

    public Input(Class<Type> typeClass, Set<LabelSpawn<?, ?>> labels) {
      super(typeClass, labels);
    }

    public void link(Operator<? extends Type> operator) {
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
    public final Hashing<? super In> hash;

    public Map(
            Set<LabelSpawn<?, ?>> labels,
            Operator<In> source,
            Class<Out> outClass,
            SerializableFunction<In, Stream<Out>> mapper
    ) {
      super(outClass, labels);
      this.source = source;
      this.mapper = mapper;
      hash = null;
    }

    public Map(Operator<In> source, Operator<In>.MapBuilder<Out> builder) {
      super(builder.outputClass, source.labels);
      this.source = source;
      mapper = builder.mapper;
      hash = builder.hash;
    }
  }

  public static final class StatefulMap<In, Key, O extends Comparable<O>, S, Out> extends Operator<Out> {
    public final Keyed<In, Key, O> keyed;
    public final SerializableBiFunction<In, S, Tuple2<S, Stream<Out>>> reducer;

    public StatefulMap(
            Keyed<In, Key, O> keyed,
            Set<LabelSpawn<?, ?>> labels,
            Class<Out> outClass,
            SerializableBiFunction<In, S, Tuple2<S, Stream<Out>>> reducer
    ) {
      super(outClass, labels);
      this.keyed = keyed;
      this.reducer = reducer;
    }
  }

  public static final class Grouping<In, Key, O extends Comparable<O>> extends Operator<List<In>> {
    public final Keyed<In, Key, O> keyed;
    public final int window;
    public final boolean undoPartialWindows;

    public Grouping(Keyed<In, Key, O> keyed, int window, boolean undoPartialWindows) {
      //noinspection unchecked
      super((Class<List<In>>) (Class<?>) List.class, keyed.key.labels());
      this.keyed = keyed;
      this.window = window;
      this.undoPartialWindows = undoPartialWindows;
    }
  }

  public static final class LabelMarkers<In, L> extends Operator<L> {
    @org.jetbrains.annotations.NotNull
    public final LabelSpawn<?, L> labelSpawn;
    public final Operator<In> source;
    public final Hashing<? super L> hashing;

    public LabelMarkers(LabelSpawn<?, L> labelSpawn, Operator<In> source, Hashing<? super L> hashing) {
      super(labelSpawn.lClass, Collections.singleton(labelSpawn));
      this.labelSpawn = labelSpawn;
      this.source = source;
      this.hashing = hashing;
    }
  }
}
