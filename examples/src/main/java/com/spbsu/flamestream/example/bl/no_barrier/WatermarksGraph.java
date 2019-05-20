package com.spbsu.flamestream.example.bl.no_barrier;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

public class WatermarksGraph {
  static class IdentityBroadcast implements Function<Integer, Stream<Integer>> {
    @Override
    public Stream<Integer> apply(Integer integer) {
      return Stream.of(integer);
    }
  }

  public static Graph apply(int parallelism, int iterations) {
    final Graph.Builder graphBuilder = new Graph.Builder();
    final Source source = new Source();
    //https://bugs.openjdk.java.net/browse/JDK-8145964
    Graph.Vertex from = new FlameMap<>(new IdentityBroadcast(), Integer.class, HashFunction.BROADCAST);
    graphBuilder.link(source, from);
    for (int iteration = 0; iteration < iterations; iteration++) {
      FlameMap<Integer, Integer> counter = new FlameMap<>(
              new Function<Integer, Stream<Integer>>() {
                final ConcurrentHashMap<Integer, Integer> counts = new ConcurrentHashMap<>();

                @Override
                public Stream<Integer> apply(Integer integer) {
                  Integer updated = counts.compute(integer, (ignored, count) -> {
                    if (count == null) {
                      count = 0;
                    }
                    count++;
                    if (count < parallelism) {
                      return count;
                    }
                    return null;
                  });
                  return updated == null ? Stream.of(integer) : Stream.empty();
                }
              },
              Integer.class,
              iteration + 1 < iterations ? HashFunction.BROADCAST : HashFunction.constantHash(0)
      );
      graphBuilder.link(from, counter);
      from = counter;
    }
    Sink sink = new Sink();
    graphBuilder.link(from, sink);
    return graphBuilder.build(source, sink);
  }
}
