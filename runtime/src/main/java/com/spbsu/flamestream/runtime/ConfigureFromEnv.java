package com.spbsu.flamestream.runtime;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

public class ConfigureFromEnv {
  public static void configureFromEnv(IntConsumer intConsumer, String key) {
    if (System.getenv().containsKey(key)) {
      intConsumer.accept(Integer.parseInt(System.getenv(key)));
    }
  }

  public static void configureFromEnv(Consumer<String> consumer, String key) {
    if (System.getenv().containsKey(key)) {
      consumer.accept(System.getenv(key));
    }
  }

  public static <T> void configureFromEnv(Consumer<T> consumer, Function<String, T> factory, String key) {
    if (System.getenv().containsKey(key)) {
      consumer.accept(factory.apply(System.getenv(key)));
    }
  }
}
