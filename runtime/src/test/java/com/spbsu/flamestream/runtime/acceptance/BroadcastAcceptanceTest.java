package com.spbsu.flamestream.runtime.acceptance;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BroadcastAcceptanceTest extends FlameAkkaSuite {
  private static Graph graph() {
    final Source source = new Source();
    final FlameMap<String, String> broadcastFilterSource =
            new FlameMap.Builder<String, String>(Stream::of, String.class)
                    .hashFunction(HashFunction.Broadcast.INSTANCE)
                    .build();
    final Grouping<String> grouping = new Grouping<>(
            HashFunction.uniformHash(HashFunction.objectHash(String.class)),
            (dataItem, dataItem2) -> dataItem.payload(String.class).equals(dataItem2.payload(String.class)),
            1,
            String.class
    );
    final FlameMap<List<String>, String> toString =
            new FlameMap.Builder<>((List<String> strings) -> Stream.of(strings.get(0)), List.class).build();
    final FlameMap<String, String> broadcastFilterSink =
            new FlameMap.Builder<String, String>(Stream::of, String.class)
                    .hashFunction(HashFunction.Broadcast.INSTANCE)
                    .build();
    final Sink sink = new Sink();

    return new Graph.Builder().link(source, broadcastFilterSource)
            .link(broadcastFilterSource, grouping)
            .link(grouping, toString)
            .link(toString, broadcastFilterSink)
            .link(broadcastFilterSink, sink)
            .build(source, sink);
  }

  @Test
  public void broadcastTest() throws InterruptedException {
    final int nodes = 4;
    final int parallelism = nodes * Runtime.getRuntime().availableProcessors();
    try (final LocalRuntime runtime = new LocalRuntime.Builder().parallelism(nodes).build()) {
      try (final FlameRuntime.Flame flame = runtime.run(graph())) {
        final List<AkkaFront.FrontHandle<String>> handles = flame
                .attachFront("broadcastFront", new AkkaFrontType<String>(runtime.system()))
                .collect(Collectors.toList());
        final int streamSize = 1000;
        final Queue<String> source = IntStream.range(0, streamSize)
                .mapToObj(String::valueOf)
                .limit(streamSize)
                .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));

        final AwaitResultConsumer<String> consumer = new AwaitResultConsumer<>(streamSize * parallelism * parallelism);
        flame.attachRear("broadcastRear", new AkkaRearType<>(runtime.system(), String.class))
                .forEach(f -> f.addListener(consumer));
        applyDataToAllHandlesAsync(source, handles);

        consumer.await(5, TimeUnit.MINUTES);
        final List<String> collect = consumer.result().collect(Collectors.toList());
        Assert.assertEquals(collect.size(), streamSize * parallelism * parallelism);

        final Map<String, Long> counter = collect.stream()
                .collect(Collectors.groupingBy(e -> e, Collectors.counting()));
        Assert.assertEquals(counter.keySet().size(), streamSize);
        for (long val : counter.values()) {
          Assert.assertEquals(val, parallelism * parallelism);
        }
      }
    }
  }
}
