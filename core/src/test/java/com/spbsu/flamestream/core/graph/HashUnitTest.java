package com.spbsu.flamestream.core.graph;

import org.testng.annotations.Test;

import java.util.stream.Collectors;

import static org.testng.Assert.*;

public class HashUnitTest {
  @Test
  public void testScale() {
    assertEquals(Integer.MIN_VALUE, HashUnit.ALL.scale(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, HashUnit.ALL.scale(Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE));
    assertEquals(Integer.MIN_VALUE, HashUnit.ALL.scale(0, 0, 0));
    assertEquals(Integer.MAX_VALUE, HashUnit.ALL.scale(1, 0, 0) - 1);
  }

  @Test
  public void testCoveringMaxValue() {
    assertTrue(HashUnit.covering(1).anyMatch(hashUnit -> hashUnit.covers(Integer.MAX_VALUE)));
    assertTrue(HashUnit.covering(2).anyMatch(hashUnit -> hashUnit.covers(Integer.MAX_VALUE)));
    assertTrue(HashUnit.covering(3).anyMatch(hashUnit -> hashUnit.covers(Integer.MAX_VALUE)));
  }
}
