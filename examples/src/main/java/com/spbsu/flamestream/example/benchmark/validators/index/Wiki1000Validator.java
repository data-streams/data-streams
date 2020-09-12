package com.spbsu.flamestream.example.benchmark.validators.index;

/**
 * User: Artem
 * Date: 03.01.2018
 */
public class Wiki1000Validator extends WikiBenchValidator {
  @Override
  public int inputLimit() {
    return 1000;
  }

  @Override
  public int expectedOutputSize() {
    return 757873;
  }
}
