package com.spbsu.flamestream.example.labels;

import org.testng.annotations.Test;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import static org.testng.Assert.*;

public class FlowFunctionTest {
  @Test
  public void testImmutable() {
    final BreadthSearchGraph.VertexIdentifier vertexIdentifier = new BreadthSearchGraph.VertexIdentifier(0);
    final ArrayList<BreadthSearchGraph.RequestOutput> output = new ArrayList<>();
    final BreadthSearchGraph.Request.Identifier requestIdentifier = new BreadthSearchGraph.Request.Identifier(0);
    new FlowFunction<>(
            BreadthSearchGraph.immutableFlow(__ -> new BreadthSearchGraph.HashedVertexEdges() {
              @Override
              public Stream<BreadthSearchGraph.VertexIdentifier> apply(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
                return Stream.empty();
              }

              @Override
              public int hash(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
                return 0;
              }
            }),
            output::add
    ).put(new BreadthSearchGraph.Request(requestIdentifier, vertexIdentifier, 1));
    assertEquals(output, Collections.singletonList(
            new BreadthSearchGraph.RequestOutput(requestIdentifier, Collections.singletonList(vertexIdentifier))
    ));
  }

  @Test
  public void testMutable() {
    final BreadthSearchGraph.VertexIdentifier vertexIdentifier = new BreadthSearchGraph.VertexIdentifier(0);
    final ArrayList<BreadthSearchGraph.RequestOutput> output = new ArrayList<>();
    final BreadthSearchGraph.Request.Identifier requestIdentifier = new BreadthSearchGraph.Request.Identifier(0);
    new FlowFunction<>(
            BreadthSearchGraph.mutableFlow(__ -> new BreadthSearchGraph.HashedVertexEdges() {
              @Override
              public Stream<BreadthSearchGraph.VertexIdentifier> apply(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
                if (vertexIdentifier.id == 0) {
                  return Stream.of(vertexIdentifier);
                }
                return Stream.empty();
              }

              @Override
              public int hash(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
                return 0;
              }
            }),
            output::add
    ).put(new BreadthSearchGraph.Request(requestIdentifier, vertexIdentifier, 1));
    assertEquals(output, Collections.singletonList(
            new BreadthSearchGraph.RequestOutput(requestIdentifier, Collections.singletonList(vertexIdentifier))
    ));
  }
}
