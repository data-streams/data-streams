package com.spbsu.flamestream.runtime.hot_keys;

import java.util.function.BiConsumer;

public interface KeyFrequency<Key> {
  void forEach(BiConsumer<Key, Double> action);
}
