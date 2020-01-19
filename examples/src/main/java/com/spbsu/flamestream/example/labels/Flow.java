package com.spbsu.flamestream.example.labels;

class Flow<In, Out> {
  final Operator.Input<? extends In> input;
  final Operator<? super Out> output;

  Flow(Operator.Input<? extends In> input, Operator<? super Out> output) {
    this.input = input;
    this.output = output;
  }
}
