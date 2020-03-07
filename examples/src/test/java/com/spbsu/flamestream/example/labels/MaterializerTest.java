package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.annotations.Test;
import scala.util.Either;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class MaterializerTest extends FlameAkkaSuite {
  @Test
  public void testForkAndJoinWithLabels() {
    final Operator.Input<Integer> input = new Operator.Input<>(Integer.class);
    final Operator.LabelSpawn<Integer, Integer> label = input.spawnLabel(Integer.class, i -> i);
    final Operator.Input<Integer> union = new Operator.Input<>(Integer.class, Collections.singleton(label));
    union.link(label.map(Integer.class, i -> i));
    union.link(label.map(Integer.class, i -> i));
    final Flow<Integer, Integer> flow = new Flow<>(input, union.labelMarkers(label));

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
  public void testImmutableBreadthSearch() throws InterruptedException {
    final Flow<BreadthSearchGraph.Request, Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.RequestKey>> flow =
            BreadthSearchGraph.immutableFlow(new HashMap<>());
    assertEquals(Materializer.buildTrackingComponents(Materializer.buildStronglyConnectedComponents(flow)
            .get(flow.output)).entrySet().stream().collect(Collectors.groupingBy(
            Map.Entry::getValue,
            Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
    )).size(), 2);

    try (final LocalRuntime runtime = new LocalRuntime.Builder().maxElementsInGraph(2)
            .millisBetweenCommits(500)
            .build()) {
      try (final FlameRuntime.Flame flame = runtime.run(Materializer.materialize(flow))) {
        final BreadthSearchGraph.VertexIdentifier vertexIdentifier = new BreadthSearchGraph.VertexIdentifier(0);
        final ArrayList<Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.RequestKey>> output = new ArrayList<>();
        final BreadthSearchGraph.Request.Identifier requestIdentifier = new BreadthSearchGraph.Request.Identifier(0);
        final Queue<BreadthSearchGraph.Request> input = new ConcurrentLinkedQueue<>();
        input.add(new BreadthSearchGraph.Request(requestIdentifier, vertexIdentifier, 1));

        final AwaitResultConsumer<Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.RequestKey>> awaitConsumer =
                new AwaitResultConsumer<>(2);
        flame.attachRear(
                "wordCountRear",
                new AkkaRearType<>(
                        runtime.system(),
                        (Class<Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.RequestKey>>) (Class<?>) Either.class
                )
        ).forEach(r -> r.addListener(awaitConsumer));
        final List<AkkaFront.FrontHandle<BreadthSearchGraph.Request>> handles = flame
                .attachFront("wordCountFront", new AkkaFrontType<BreadthSearchGraph.Request>(runtime.system()))
                .collect(Collectors.toList());
        applyDataToAllHandlesAsync(input, handles);
        awaitConsumer.await(200, TimeUnit.SECONDS);

        final BreadthSearchGraph.RequestOutput actualWordsTop = awaitConsumer.result().findFirst().get().left().get();
      }
    }
  }

  @Test
  public void testMutableBreadthSearch() {
    final Flow<BreadthSearchGraph.Input, Either<BreadthSearchGraph.RequestOutput, BreadthSearchGraph.RequestKey>> flow =
            BreadthSearchGraph.mutableFlow(new HashMap<>());
    assertEquals(
            Materializer.buildTrackingComponents(Materializer.buildStronglyConnectedComponents(flow).get(flow.output))
                    .entrySet().stream().collect(Collectors.groupingBy(
                    Map.Entry::getValue,
                    Collectors.mapping(Map.Entry::getKey, Collectors.toSet())
            )).size(),
            2
    );
  }
}
