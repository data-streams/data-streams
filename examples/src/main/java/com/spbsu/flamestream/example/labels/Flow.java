package com.spbsu.flamestream.example.labels;

class Flow<In, Out> {
  final Operator.Input<In> input;
  final Operator<Out> output;

  Flow(Operator.Input<In> input, Operator<Out> output) {
    this.input = input;
    this.output = output;
  }
}
