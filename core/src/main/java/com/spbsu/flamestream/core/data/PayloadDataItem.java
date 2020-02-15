package com.spbsu.flamestream.core.data;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Labels;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class PayloadDataItem implements DataItem {
  private final Meta meta;
  private final Labels labels;
  private final Object payload;
  private final long xor;

  public PayloadDataItem(Meta meta, Object payload) {
    this.payload = payload;
    this.meta = meta;
    labels = new Labels(1);
    this.xor = ThreadLocalRandom.current().nextLong();
  }

  public PayloadDataItem(Meta meta, Object payload, Labels labels) {
    this.payload = payload;
    this.meta = meta;
    this.labels = labels;
    this.xor = ThreadLocalRandom.current().nextLong();
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
  public Labels labels() {
    return labels;
  }

  @Override
  public long xor() {
    return xor;
  }

  @Override
  public DataItem cloneWith(Meta newMeta) {
    return new PayloadDataItem(newMeta, this.payload);
  }

  @Override
  public String toString() {
    return '{' + "meta=" + meta + ", payload=" + payload + '}';
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
            Objects.equals(payload, that.payload) &&
            Objects.equals(labels, that.labels);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, payload, xor, labels);
  }
}