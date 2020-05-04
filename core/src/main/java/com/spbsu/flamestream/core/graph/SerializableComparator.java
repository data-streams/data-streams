package com.spbsu.flamestream.core.graph;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

public interface SerializableComparator<T> extends Serializable, Comparator<T> {
  static <T, U extends Comparable<? super U>> SerializableComparator<T> comparing(
          SerializableFunction<? super T, ? extends U> keyExtractor
  ) {
    Objects.requireNonNull(keyExtractor);
    return (c1, c2) -> keyExtractor.apply(c1).compareTo(keyExtractor.apply(c2));
  }

  default SerializableComparator<T> thenComparing(SerializableComparator<? super T> other) {
    Objects.requireNonNull(other);
    return (c1, c2) -> {
      final int res = this.compare(c1, c2);
      return res != 0 ? res : other.compare(c1, c2);
    };
  }

  default <U extends Comparable<? super U>> SerializableComparator<T> thenComparing(
          SerializableFunction<? super T, ? extends U> keyExtractor
  ) {
    return thenComparing(comparing(keyExtractor));
  }
}
