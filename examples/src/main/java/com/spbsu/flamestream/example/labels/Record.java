package com.spbsu.flamestream.example.labels;

public class Record<Value> {
  final Value value;
  final Labels labels;

  public Record(Value value, Labels labels) {
    this.value = value;
    this.labels = labels;
  }
}
