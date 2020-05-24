package com.spbsu.flamestream.core.graph;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBiFunction<T, U, R> extends Serializable, BiFunction<T, U, R> {}
