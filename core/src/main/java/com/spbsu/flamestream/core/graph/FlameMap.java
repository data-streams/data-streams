package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.LabelsPresence;
import com.spbsu.flamestream.core.data.meta.Meta;
import org.jetbrains.annotations.Nullable;

import java.util.stream.Stream;

public class FlameMap<T, R> extends HashingVertexStub {
  public static class Builder<T, R> {
    private final SerializableFunction<T, Stream<R>> function;
    private final Class<?> clazz;
    private @Nullable
    HashFunction hashFunction;
    private SerializableConsumer<HashGroup> init = __ -> {};
    private LabelsPresence labelsPresence = LabelsPresence.EMPTY;

    public Builder(SerializableFunction<T, Stream<R>> function, Class<?> clazz) {
      this.function = function;
      this.clazz = clazz;
    }

    public Builder<T, R> hashFunction(@Nullable HashFunction hashFunction) {
      this.hashFunction = hashFunction;
      return this;
    }

    public Builder<T, R> init(SerializableConsumer<HashGroup> init) {
      this.init = init;
      return this;
    }

    public Builder<T, R> labelsPresence(LabelsPresence labelsPresence) {
      this.labelsPresence = labelsPresence;
      return this;
    }

    public FlameMap<T, R> build() {
      return new FlameMap<>(this);
    }
  }

  private final SerializableFunction<T, Stream<R>> function;
  private final Class<?> clazz;
  private final @Nullable
  HashFunction hashFunction;
  private final SerializableConsumer<HashGroup> init;
  private final LabelsPresence labelsPresence;

  private FlameMap(Builder<T, R> builder) {
    function = builder.function;
    clazz = builder.clazz;
    hashFunction = builder.hashFunction;
    init = builder.init;
    labelsPresence = builder.labelsPresence;
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

  public SerializableFunction<T, Stream<R>> function() {
    return function;
  }

  public void init(HashGroup hashGroup) {
    init.accept(hashGroup);
  }

  @Override
  public @Nullable
  HashFunction hash() {
    return hashFunction;
  }

  public class FlameMapOperation {
    private final long physicalId;

    FlameMapOperation(long physicalId) {
      this.physicalId = physicalId;
    }

    public Stream<DataItem> apply(DataItem dataItem) {
      //noinspection unchecked
      final Stream<R> result = function.apply(dataItem.payload((Class<T>) clazz));
      final int[] childId = {0};
      final boolean hasLabels = dataItem.meta().labels().hasAll(labelsPresence);
      return result.map(r -> {
        if (!hasLabels) {
          throw new IllegalArgumentException();
        }
        return new PayloadDataItem(new Meta(dataItem.meta(), physicalId, childId[0]++), r);
      });
    }
  }
}
