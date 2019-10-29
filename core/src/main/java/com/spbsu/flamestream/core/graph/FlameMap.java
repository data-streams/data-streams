package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class FlameMap<T, R> extends HashingVertexStub {
  private final Function<T, Stream<R>> function;
  private final Class<?> clazz;
  private final @Nullable
  HashFunction hashFunction;
  private final @Nullable
  Runnable init;

  public FlameMap(Function<T, Stream<R>> function,
                  Class<?> clazz,
                  @Nullable HashFunction hashFunction,
                  @Nullable Runnable init) {
    this.function = function;
    this.clazz = clazz;
    this.hashFunction = hashFunction;
    this.init = init;
  }

  public FlameMap(Function<T, Stream<R>> function, Class<?> clazz, @Nullable Runnable init) {
    this(function, clazz, null, init);
  }

  public FlameMap(Function<T, Stream<R>> function, Class<?> clazz, @Nullable HashFunction hashFunction) {
    this(function, clazz, hashFunction, null);
  }

  public FlameMap(Function<T, Stream<R>> function, Class<?> clazz) {
    this(function, clazz, null, null);
  }

  public FlameMapOperation operation(long physicalId) {
    return new FlameMapOperation(physicalId);
  }

  @Override
  public String toString() {
    return "FlameMap{" +
            "function=" + function +
            '}';
  }

  public Function<T, Stream<R>> function() {
    return function;
  }

  public void init() {
    if (init != null) {
      init.run();
    }
  }

  @Override
  public @Nullable
  HashFunction hash() {
    return hashFunction;
  }

  public Stream<R> apply(T value, Consumer<Supplier<Stream<R>>> scheduleDoneSnapshot) {
    return function.apply(value);
  }

  public class FlameMapOperation {
    private final long physicalId;

    FlameMapOperation(long physicalId) {
      this.physicalId = physicalId;
    }

    public Stream<DataItem> apply(DataItem dataItem,
                                  int vertexIndex,
                                  Consumer<Supplier<Stream<DataItem>>> scheduleDoneSnapshot) {
      final int[] childId = {0};
      final Function<R, DataItem> rDataItemFunction = r -> new PayloadDataItem(new Meta(
              dataItem.meta(),
              physicalId,
              childId[0]++,
              new GlobalTime(dataItem.meta().globalTime().time(), dataItem.meta().globalTime().frontId(), vertexIndex)
      ), r);
      //noinspection unchecked
      return FlameMap.this.apply(
              dataItem.payload((Class<T>) clazz),
              onDoneSnapshot -> scheduleDoneSnapshot.accept(() -> onDoneSnapshot.get().map(rDataItemFunction))
      ).map(rDataItemFunction);
    }
  }
}
