package com.spbsu.benchmark.flink.roundrobin.ops;

import org.apache.flink.api.common.functions.RichMapFunction;

public class IntermediateVertex extends RichMapFunction<Integer, Integer> {
  @Override
  public Integer map(Integer value) {
    return value;
  }
}
