package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.data.meta.Labels;
import com.spbsu.flamestream.core.data.meta.Meta;

public interface DataItem {
  Meta meta();

  <T> T payload(Class<T> expectedClass);

  Labels labels();

  long xor();

  DataItem cloneWith(Meta meta);

  boolean marker();
}
