package com.spbsu.flamestream.core.graph;

import java.io.Serializable;
import java.util.function.Predicate;

public interface SerializablePredicate<T> extends Serializable, Predicate<T> {}
