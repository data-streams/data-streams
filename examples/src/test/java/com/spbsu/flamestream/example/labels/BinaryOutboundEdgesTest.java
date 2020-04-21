package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.graph.HashGroup;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;

public class BinaryOutboundEdgesTest {
  @Test
  public void test() throws IOException {
    //System.out.println(new BinaryOutboundEdges(
    //        new File("/Users/faucct/Code/flamestream/tail_head_offset.bin"),
    //        new File("/Users/faucct/Code/flamestream/edge_head.bin"),
    //        new HashGroup(Collections.singleton(HashUnit.covering(4).findFirst().get()))
    //).apply(new BreadthSearchGraph.VertexIdentifier(13)).collect(Collectors.toList()));
    System.out.println(new BinarySocialGraph(
            new File("/Users/faucct/Code/flamestream/tail_head_offset.bin"),
            new File("/Users/faucct/Code/flamestream/edge_head.bin")
    ).new BinaryOutboundEdges(new HashGroup(Collections.emptySet()), Integer.MAX_VALUE)
            .apply(new BreadthSearchGraph.VertexIdentifier(13)).collect(Collectors.toList()));
  }
}
