package com.spbsu.flamestream.example.bl;

import com.google.common.hash.Hashing;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.config.HashUnit;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class WatermarksVsAckerGraph {
  static public abstract class Element {
    public final int id;

    protected Element(int id) {
      this.id = id;
    }
  }

  static public final class Data extends Element {
    private final int hash;

    public Data(int id) {
      this(id, Hashing.murmur3_32().hashInt(id).asInt());
    }

    public Data(int id, int hash) {
      super(id);
      this.hash = hash;
    }
  }

  static public final class Child extends Element {
    private final int hash;
    final int childId;

    Child(int parentId, int childId) {
      super(parentId);
      this.childId = childId;
      this.hash = Hashing.murmur3_32().newHasher(8).putInt(parentId).putInt(childId).hash().asInt();
    }
  }

  static public final class Watermark extends Element {
    public final int fromPartition, toPartition;

    public Watermark(int id, int toPartition) {
      this(id, 0, toPartition);
    }

    public Watermark(int id, int fromPartition, int toPartition) {
      super(id);
      this.fromPartition = fromPartition;
      this.toPartition = toPartition;
    }

    @Override
    public String toString() {
      return "Watermark(id = " + id + ", partition = " + toPartition + ")";
    }
  }

  private static Stream<Element> watermarkStream(int parallelism, int id, int fromPartition) {
    return IntStream.range(0, parallelism).mapToObj(toPartition -> new Watermark(
            id,
            fromPartition,
            toPartition
    ));
  }

  private static class Iteration implements Function<Element, Stream<Element>> {
    final int fromPartitions, toPartitions;
    final int[] watermarks;
    int lastEmitted = Integer.MIN_VALUE;

    Iteration(int fromPartitions, int toPartitions) {
      this.fromPartitions = fromPartitions;
      this.toPartitions = toPartitions;
      watermarks = new int[fromPartitions];
      Arrays.fill(watermarks, lastEmitted);
    }

    @Override
    public Stream<Element> apply(Element element) {
      if (element instanceof Data) {
        return Stream.of(element);
      }
      if (element instanceof Watermark) {
        final Watermark incoming = (Watermark) element;
        if (watermarks[incoming.fromPartition] < incoming.id) {
          watermarks[incoming.fromPartition] = incoming.id;
          int watermarkToEmit = watermarks[0];
          for (final int watermark : watermarks) {
            watermarkToEmit = Math.min(watermarkToEmit, watermark);
          }
          if (lastEmitted < watermarkToEmit) {
            lastEmitted = watermarkToEmit;
            return watermarkStream(toPartitions, watermarkToEmit, incoming.toPartition);
          }
        }
        return Stream.empty();
      }
      throw new IllegalArgumentException(element.toString());
    }
  }

  private static class HashFunction implements com.spbsu.flamestream.core.HashFunction {
    final List<HashUnit> covering;
    final int iteration;

    HashFunction(List<HashUnit> covering, int iteration) {
      this.covering = covering;
      this.iteration = iteration;
    }

    @Override
    public int hash(DataItem dataItem) {
      Element payload = dataItem.payload(Element.class);
      if (payload instanceof Watermark) {
        return covering.get(((Watermark) payload).toPartition).from();
      }
      final int hash;
      if (payload instanceof Data) {
        hash = ((Data) payload).hash;
      } else {
        hash = ((Child) payload).hash;
      }
      return Hashing.murmur3_32().newHasher(8).putInt(hash).putInt(iteration).hash().asInt();
    }
  }

  @SuppressWarnings("Convert2Lambda")
  public static Graph apply(List<HashUnit> covering, int iterations, int childrenNumber) {
    final Graph.Builder graphBuilder = new Graph.Builder();
    final Source source = new Source();
    final Sink sink = new Sink();
    final Graph.Vertex start = new FlameMap<>(new Iteration(1, covering.size()), Element.class);
    final Graph.Vertex end = new FlameMap<>(
            new Iteration(covering.size(), 1),
            Element.class,
            new HashFunction(covering, iterations)
    );

    graphBuilder
            .link(source, start)
            .link(
                    IntStream.range(0, iterations).boxed().<Graph.Vertex>map(iteration -> new FlameMap<>(
                            new Iteration(covering.size(), covering.size()),
                            Element.class,
                            new HashFunction(covering, iteration)
                    )).reduce(start, (from, to) -> {
                      graphBuilder.link(from, to);
                      return to;
                    }),
                    end
            )
            .link(end, sink);
    return graphBuilder.build(source, sink);
  }
}
