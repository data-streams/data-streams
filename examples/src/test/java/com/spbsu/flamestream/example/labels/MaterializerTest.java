package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.TrackingComponent;
import org.testng.annotations.Test;
import scala.util.Either;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

public class MaterializerTest {
  @Test
  public void testForkAndJoinWithLabels() {
    final Operator.Input<Integer> input = new Operator.Input<>();
    final Operator<Integer> labeled = input.spawnLabel(Integer.class, Function.identity());
    final Operator.Input<Integer> union = new Operator.Input<>(Collections.singleton(Integer.class));
    union.link(labeled.map(Function.identity()));
    union.link(labeled.map(Function.identity()));
    final Flow<Integer, Integer> flow = new Flow<>(input, union.labelMarkers(Integer.class));

    final Map<Operator<?>, Materializer.StronglyConnectedComponent> stronglyConnectedComponents =
            Materializer.buildStronglyConnectedComponents(flow);
    assertEquals(new HashSet<>(stronglyConnectedComponents.values()).size(), stronglyConnectedComponents.size());
    final Map<Materializer.StronglyConnectedComponent, TrackingComponent> trackingComponents =
            Materializer.buildTrackingComponents(stronglyConnectedComponents.get(flow.output));
    final Map<TrackingComponent, Set<Materializer.StronglyConnectedComponent>> trackingComponentStrongComponents =
            trackingComponents.entrySet().stream().collect(Collectors.groupingBy(
                    Map.Entry::getValue,
                    Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
            ));
    assertEquals(trackingComponentStrongComponents.size(), 2);
    assertEquals(
            trackingComponentStrongComponents.get(
                    trackingComponents.get(stronglyConnectedComponents.get(flow.output))
            ).size(),
            1
    );
  }

  @Test
  public void testImmutableBreadthSearch() {
    final Flow<BreadthSearchGraph.Request, Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.RequestKey>> flow =
            BreadthSearchGraph.immutableFlow(new HashMap<>());
    assertEquals(Materializer.buildTrackingComponents(Materializer.buildStronglyConnectedComponents(flow)
            .get(flow.output)).entrySet().stream().collect(Collectors.groupingBy(
            Map.Entry::getValue,
            Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
    )).size(), 2);
  }

  @Test
  public void testMutableBreadthSearch() {
    final Flow<BreadthSearchGraph.Input, Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.RequestKey>> flow =
            BreadthSearchGraph.mutableFlow(new HashMap<>());
    assertEquals(Materializer.buildTrackingComponents(Materializer.buildStronglyConnectedComponents(flow)
            .get(flow.output)).entrySet().stream().collect(Collectors.groupingBy(
            Map.Entry::getValue,
            Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
    )).size(), 2);
  }
}
