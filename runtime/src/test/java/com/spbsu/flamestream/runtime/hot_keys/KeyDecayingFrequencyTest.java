package com.spbsu.flamestream.runtime.hot_keys;

import org.testng.annotations.Test;

import java.util.HashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class KeyDecayingFrequencyTest {
  @Test
  public void test() {
    final KeyDecayingFrequency<Integer> keyFrequency = new KeyDecayingFrequency<>(
            1,
            1000,
            0.5
    );
    keyFrequency.add(1, 1);
    keyFrequency.add(2, 1);
    keyFrequency.add(3, 1);
    final HashMap<Integer, Double> map = new HashMap<>();
    keyFrequency.forEach(map::put);
    assertEquals(map.size(), 1);
    assertEquals(map.get(3), 1.0 + 1.0);
  }

  @Test
  public void testDecay() {
    final KeyDecayingFrequency<Integer> keyFrequency = new KeyDecayingFrequency<>(
            1,
            1,
            0.5
    );
    keyFrequency.add(1, 1);
    {
      final HashMap<Integer, Double> map = new HashMap<>();
      keyFrequency.forEach(map::put);
      assertEquals(map.size(), 1);
      assertEquals(map.get(1), 0.5);
    }
    keyFrequency.add(1, 0);
    {
      final HashMap<Integer, Double> map = new HashMap<>();
      keyFrequency.forEach(map::put);
      assertTrue(map.isEmpty());
    }
  }
}
