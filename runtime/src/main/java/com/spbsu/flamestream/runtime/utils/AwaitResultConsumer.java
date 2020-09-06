package com.spbsu.flamestream.runtime.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class AwaitResultConsumer<T> implements Consumer<T> {
  private final Collection<T> result;
  public final int expectedSize;
  private boolean finished = false;

  public AwaitResultConsumer(int expectedSize) {
    this(expectedSize, ArrayList::new);
  }

  public AwaitResultConsumer(int expectedSize, Supplier<Collection<T>> supplier) {
    this.expectedSize = expectedSize;
    result = supplier.get();
  }

  @Override
  public void accept(T value) {
    synchronized (result) {
      result.add(value);
      if (result.size() == expectedSize && !finished) {
        finished = true;
        result.notifyAll();
      } else if (result.size() > expectedSize) {
        throw new IllegalStateException("Accepted more than expected");
      }
    }
  }

  public void await(long timeout, TimeUnit unit) throws InterruptedException {
    final long stop = System.currentTimeMillis() + unit.toMillis(timeout);
    synchronized (result) {
      while (result.size() < expectedSize && System.currentTimeMillis() < stop && !finished) {
        result.wait(unit.toMillis(timeout));
      }
    }
  }

  public Stream<T> result() {
    synchronized (result) {
      return result.stream();
    }
  }
}
