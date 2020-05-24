package com.spbsu.flamestream.core.graph;

import java.io.Serializable;
import java.util.function.ToIntFunction;

public interface SerializableToIntFunction<T> extends Serializable, ToIntFunction<T> {}
