package com.spbsu.flamestream.runtime.hot_keys;

import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.*;

public class HeuristicsTest {

  @Test
  public void testAnyProblems() {
    final Heuristics.Partitions<Integer> twoPartitions = new Heuristics.Partitions<>() {
      @Override
      public int size() {
        return 2;
      }

      @Override
      public int map(Integer integer) {
        return integer;
      }
    };
    assertTrue(new Heuristics(0, 10).anyProblems(Map.of(
            0, 1.0,
            1, 11.0
    )::forEach, twoPartitions));
    assertFalse(new Heuristics(100, 10).anyProblems(Map.of(
            0, 1.0,
            1, 11.0
    )::forEach, twoPartitions));
    assertFalse(new Heuristics(0, 10).anyProblems(Map.of(
            0, 1.0,
            1, 9.0
    )::forEach, twoPartitions));
  }
}