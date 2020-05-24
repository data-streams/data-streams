package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.function.BiPredicate;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class DataItemForTest implements DataItem {
  private final DataItem inner;
  private final HashFunction hash;
  private final BiPredicate<DataItem, DataItem> equalz;


  DataItemForTest(DataItem inner, HashFunction hash, BiPredicate<DataItem, DataItem> equalz) {
    this.inner = inner;
    this.hash = hash;
    this.equalz = equalz;
  }

  @Override
  public Meta meta() {
    return inner.meta();
  }

  @Override
  public <R> R payload(Class<R> expectedClass) {
    return inner.payload(expectedClass);
  }

  @Override
  public long xor() {
    return inner.xor();
  }

  @Override
  public DataItem cloneWith(Meta meta) {
    return new DataItemForTest(inner.cloneWith(meta), hash, equalz);
  }

  @Override
  public boolean marker() {
    return inner.marker();
  }

  @Override
  public int hashCode() {
    return hash.applyAsInt(inner);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DataItem that = (DataItem) o;
    return equalz.test(inner, that);
  }
}
