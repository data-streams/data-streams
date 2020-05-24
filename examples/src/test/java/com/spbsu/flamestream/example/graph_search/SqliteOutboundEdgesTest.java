package com.spbsu.flamestream.example.graph_search;

import com.spbsu.flamestream.example.graph_search.BreadthSearchGraph;
import com.spbsu.flamestream.example.graph_search.SqliteOutboundEdges;
import org.testng.annotations.Test;

public class SqliteOutboundEdgesTest {
  @Test
  public void test() {
    System.out.println(SqliteOutboundEdges.INSTANCE.apply(new BreadthSearchGraph.VertexIdentifier(13)));
  }
}
