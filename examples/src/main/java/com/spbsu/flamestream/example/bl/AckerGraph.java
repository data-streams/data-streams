package com.spbsu.flamestream.example.bl;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;

import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class AckerGraph {
  static class Identity implements Function<Integer, Stream<Integer>> {
    @Override
    public Stream<Integer> apply(Integer integer) {
      return Stream.of(integer);
    }
  }

  public static Graph apply(int iterations) {
    final Graph.Builder graphBuilder = new Graph.Builder();
    final Source source = new Source();
    Sink sink = new Sink();
    //https://bugs.openjdk.java.net/browse/JDK-8145964
    final Graph.Vertex start = new FlameMap<>(new Identity(), Integer.class, HashFunction.objectHash(Integer.class));
    //noinspection Convert2Lambda
    graphBuilder
            .link(source, start)
            .link(IntStream.range(0, iterations).boxed().<Graph.Vertex>map(iteration -> new FlameMap<>(
                    new Identity(),
                    Integer.class,
                    HashFunction.uniformHash(new HashFunction() {
                      @Override
                      public int hash(DataItem dataItem) {
                        return dataItem.payload(Integer.class) + iteration;
                      }
                    })
            )).reduce(start, (from, to) -> {
              graphBuilder.link(from, to);
              return to;
            }), sink);
    return graphBuilder.build(source, sink);
  }
}
