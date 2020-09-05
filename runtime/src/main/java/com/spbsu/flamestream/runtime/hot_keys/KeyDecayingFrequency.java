package com.spbsu.flamestream.runtime.hot_keys;

import com.google.common.collect.TreeMultimap;

import java.util.HashMap;
import java.util.Objects;
import java.util.function.BiConsumer;

public class KeyDecayingFrequency<Key extends Comparable<Key>> implements KeyFrequency<Key> {
  private final int capacity;
  private long epochCounter = 0;
  private final long epochLength;
  private final double decayingFactor;
  private final HashMap<Key, Double> keyFrequency = new HashMap<>();
  private final TreeMultimap<Double, Key> frequencyKeys = TreeMultimap.create();
  private double restFrequency = 0;

  public KeyDecayingFrequency(
          int capacity,
          long epochLength,
          double decayingFactor
  ) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("" + capacity);
    }
    if (epochLength <= 0) {
      throw new IllegalArgumentException("" + epochLength);
    }
    if (decayingFactor < 0 || decayingFactor > 1) {
      throw new IllegalArgumentException("" + decayingFactor);
    }
    this.capacity = capacity;
    this.epochLength = epochLength;
    this.decayingFactor = decayingFactor;
  }

  public void add(Key key, double change) {
    keyFrequency.compute(key, (ignored, prevFrequency) -> {
      if (prevFrequency != null) {
        return changeKeyFrequency(key, prevFrequency, prevFrequency + change);
      } else if (keyFrequency.size() < capacity) {
        Double newFrequency = change;
        {
          final boolean put = frequencyKeys.put(newFrequency, key);
          assert put;
        }
        return newFrequency;
      } else {
        restFrequency += change;
        if (frequencyKeys.keySet().first() < restFrequency) {
          Double newFrequency = restFrequency;
          restFrequency = 0;
          {
            final boolean put = frequencyKeys.put(newFrequency, key);
            assert put;
          }
          return newFrequency;
        } else {
          return null;
        }
      }
    });
    if (capacity < keyFrequency.size()) {
      final Double minFrequency = frequencyKeys.keySet().first();
      keyFrequency.remove(frequencyKeys.get(minFrequency).pollFirst());
      restFrequency += minFrequency;
    }
    tick();
  }

  private Double changeKeyFrequency(Key key, Double prevFrequency, Double newFrequency) {
    {
      final boolean removed = frequencyKeys.remove(prevFrequency, key);
      assert removed;
    }
    if (newFrequency != null) {
      final boolean put = frequencyKeys.put(newFrequency, key);
      assert put;
    }
    return newFrequency;
  }

  private void tick() {
    if (++epochCounter == epochLength) {
      epochCounter = 0;
      keyFrequency.replaceAll((key, frequency) -> changeKeyFrequency(
              key,
              frequency,
              frequency < 1 ? null : frequency * decayingFactor
      ));
      keyFrequency.values().removeIf(Objects::isNull);
    }
  }

  @Override
  public void forEach(BiConsumer<Key, Double> action) {
    keyFrequency.forEach(action);
  }
}
