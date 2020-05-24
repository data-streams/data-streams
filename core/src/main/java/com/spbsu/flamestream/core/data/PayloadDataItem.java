package com.spbsu.flamestream.core.data;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Labels;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class PayloadDataItem implements DataItem {
  private final Meta meta;
  private final Object payload;
  private final long xor;
  private final boolean marker;

  public PayloadDataItem(Meta meta, Object payload) {
    this(meta, payload, false);
  }

  public PayloadDataItem(Meta meta, Object payload, boolean marker) {
    this.meta = meta;
    this.payload = payload;
    this.xor = ThreadLocalRandom.current().nextLong();
    this.marker = marker;
  }

  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public <T> T payload(Class<T> clazz) {
    return clazz.cast(payload);
  }

  @Override
  public long xor() {
    return xor;
  }

  @Override
  public DataItem cloneWith(Meta newMeta) {
    return new PayloadDataItem(newMeta, payload, marker);
  }

  @Override
  public boolean marker() {
    return marker;
  }

  @Override
  public String toString() {
    return '{' + "meta=" + meta + ", xor=" + xor + ", payload=" + payload + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PayloadDataItem that = (PayloadDataItem) o;
    return xor == that.xor &&
            Objects.equals(meta, that.meta) &&
            Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, payload, xor);
  }
}