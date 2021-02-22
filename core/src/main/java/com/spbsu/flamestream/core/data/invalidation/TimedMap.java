package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.core.graph.SerializableSupplier;
import com.spbsu.flamestream.core.graph.SerializableToLongFunction;
import org.jetbrains.annotations.Nullable;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TimedMap<Key, Value> {
  private final ConcurrentMap<Key, Value> buffers;
  private final TreeMap<Long, Set<Key>> timeKeys = new TreeMap<>();
  private final SerializableSupplier<Value> defaultValue;
  @Nullable
  private final SerializableToLongFunction<Key> keyTime;
  private long minTime = Long.MIN_VALUE;

  public TimedMap(SerializableSupplier<Value> defaultValue, @Nullable SerializableToLongFunction<Key> keyTime) {
    this.defaultValue = defaultValue;
    this.keyTime = keyTime;
    buffers = new ConcurrentHashMap<>();
  }

  private TimedMap(
          SerializableSupplier<Value> defaultValue,
          @Nullable SerializableToLongFunction<Key> keyTime,
          ConcurrentMap<Key, Value> buffers
  ) {
    this.defaultValue = defaultValue;
    this.keyTime = keyTime;
    this.buffers = buffers;
  }

  public Value get(Key key) {
    if (keyTime != null) {
      final var time = keyTime.applyAsLong(key);
      if (time < minTime) {
        throw new IllegalArgumentException();
      }
      timeKeys.computeIfAbsent(time, __ -> new HashSet<>()).add(key);
    }
    return buffers.computeIfAbsent(key, __ -> defaultValue.get());
  }

  public void onMinTime(long minTime) {
    if (minTime <= this.minTime) {
      throw new IllegalArgumentException();
    }
    this.minTime = minTime;
    while (!timeKeys.isEmpty()) {
      final Map.Entry<Long, Set<Key>> minTimeKeys = timeKeys.firstEntry();
      if (minTime <= minTimeKeys.getKey()) {
        break;
      }
      for (Key key : minTimeKeys.getValue()) {
        buffers.remove(key);
      }
      timeKeys.pollFirstEntry();
    }
  }

  public <Output> TimedMap<Key, Output> map(SerializableFunction<Value, Output> mapper) {
    final ConcurrentMap<Key, Output> subState = new ConcurrentHashMap<>();
    buffers.forEach((key, value) -> subState.put(key, mapper.apply(value)));
    return new TimedMap<>(() -> mapper.apply(defaultValue.get()), keyTime, subState);
  }
}
