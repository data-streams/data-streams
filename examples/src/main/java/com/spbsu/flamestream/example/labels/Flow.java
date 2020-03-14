package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.graph.HashGroup;
import com.spbsu.flamestream.core.graph.SerializableConsumer;

class Flow<In, Out> {
  public final Operator.Input<In> input;
  public final Operator<Out> output;
  public final SerializableConsumer<HashGroup> init;

  public Flow(Operator.Input<In> input, Operator<Out> output) {
    this(input, output, __ -> {});
  }

  public Flow(Operator.Input<In> input, Operator<Out> output, SerializableConsumer<HashGroup> init) {
    this.input = input;
    this.output = output;
    this.init = init;
  }
}
