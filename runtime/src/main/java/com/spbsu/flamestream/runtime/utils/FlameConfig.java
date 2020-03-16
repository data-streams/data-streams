package com.spbsu.flamestream.runtime.utils;

import akka.util.Timeout;

import java.util.concurrent.TimeUnit;

public class FlameConfig {
  public static final FlameConfig config = new FlameConfig(
          Timeout.apply(5, TimeUnit.SECONDS),
          Timeout.apply(1000, TimeUnit.SECONDS)
  );
  private final Timeout smallTimeout;
  private final Timeout bigTimeout;

  private FlameConfig(Timeout smallTimeout, Timeout bigTimeout) {
    this.smallTimeout = smallTimeout;
    this.bigTimeout = bigTimeout;
  }

  public Timeout smallTimeout() {
    return smallTimeout;
  }

  public Timeout bigTimeout() {
    return bigTimeout;
  }
}
