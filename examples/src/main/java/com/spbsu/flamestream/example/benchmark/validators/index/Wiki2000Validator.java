package com.spbsu.flamestream.example.benchmark.validators.index;

/**
 * User: Artem
 * Date: 03.01.2018
 */
public class Wiki2000Validator extends WikiBenchValidator {
  @Override
  public int inputLimit() {
    return 2000;
  }

  @Override
  public int expectedOutputSize() {
    return 1451290;
  }
}
