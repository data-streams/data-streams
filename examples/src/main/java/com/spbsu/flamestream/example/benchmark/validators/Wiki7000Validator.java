package com.spbsu.flamestream.example.benchmark.validators;

/**
 * User: Artem
 * Date: 03.01.2018
 */
public class Wiki7000Validator extends WikiBenchValidator {
  @Override
  public int inputLimit() {
    return 7000;
  }

  @Override
  public int expectedOutputSize() {
    return 3050003;
  }
}
