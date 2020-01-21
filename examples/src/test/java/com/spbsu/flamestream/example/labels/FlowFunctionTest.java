package com.spbsu.flamestream.example.labels;

import org.testng.annotations.Test;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.testng.Assert.*;

public class FlowFunctionTest {
  @Test
  public void testImmutable() {
    final BreadthSearchGraph.VertexIdentifier vertexIdentifier = new BreadthSearchGraph.VertexIdentifier();
    final ArrayList<Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.RequestKey>> output = new ArrayList<>();
    final BreadthSearchGraph.Request.Identifier requestIdentifier = new BreadthSearchGraph.Request.Identifier();
    new FlowFunction<>(
            BreadthSearchGraph.immutableFlow(Collections.singletonMap(
                    vertexIdentifier,
                    Collections.singletonList(vertexIdentifier)
            )),
            output::add
    ).put(new BreadthSearchGraph.Request(requestIdentifier, vertexIdentifier, 1));
    assertEquals(output, Arrays.asList(
            new Left<>(new BreadthSearchGraph.RequestOutput(requestIdentifier, vertexIdentifier)),
            new Right<>(new BreadthSearchGraph.RequestKey(requestIdentifier))
    ));
  }

  @Test
  public void testMutable() {
    final BreadthSearchGraph.VertexIdentifier vertexIdentifier = new BreadthSearchGraph.VertexIdentifier();
    final ArrayList<Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.RequestKey>> output = new ArrayList<>();
    final BreadthSearchGraph.Request.Identifier requestIdentifier = new BreadthSearchGraph.Request.Identifier();
    new FlowFunction<>(
            BreadthSearchGraph.mutableFlow(Collections.singletonMap(
                    vertexIdentifier,
                    Collections.singletonList(vertexIdentifier)
            )),
            output::add
    ).put(new BreadthSearchGraph.Request(requestIdentifier, vertexIdentifier, 1));
    assertEquals(output, Arrays.asList(
            new Left<>(new BreadthSearchGraph.RequestOutput(requestIdentifier, vertexIdentifier)),
            new Right<>(new BreadthSearchGraph.RequestKey(requestIdentifier))
    ));
  }
}
