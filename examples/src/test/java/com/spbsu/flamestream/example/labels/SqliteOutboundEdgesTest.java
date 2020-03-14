package com.spbsu.flamestream.example.labels;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class SqliteOutboundEdgesTest {
  @Test
  public void test() {
    System.out.println(SqliteOutboundEdges.INSTANCE.apply(new BreadthSearchGraph.VertexIdentifier(13)));
  }
}
