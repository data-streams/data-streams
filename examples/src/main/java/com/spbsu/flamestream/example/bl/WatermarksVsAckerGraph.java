package com.spbsu.flamestream.example.bl;

import com.google.common.hash.Hashing;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.config.Snapshots;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
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
    public Data(int id) {
      super(id);
    }

    @Override
    public String toString() {
      return "Data(id = " + id + ")";
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

  private static class Iteration extends FlameMap<Element, Element> {
    final int fromPartitions, toPartitions;
    final int[] watermarks;
    int lastEmitted;
    private final int index;
    Snapshots<Element> initializedSnapshots;

    Iteration(int fromPartitions, int toPartitions, int defaultMinimalTime, HashFunction hashFunction, int index) {
      super(null, Element.class, hashFunction);
      this.fromPartitions = fromPartitions;
      this.toPartitions = toPartitions;
      watermarks = new int[fromPartitions];
      lastEmitted = defaultMinimalTime;
      Arrays.fill(watermarks, lastEmitted);
      this.index = index;
    }

    @Override
    public Stream<Element> apply(Element element, Consumer<Supplier<Stream<Element>>> done) {
      if (snapshots() != null && snapshots().putIfBlocked(element)) {
        return Stream.empty();
      }
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
            final Stream<Element> watermarks = watermarkStream(toPartitions, watermarkToEmit, incoming.toPartition);
            if (snapshots() != null) {
              snapshots().minTimeUpdate(
                      watermarkToEmit + 1,
                      supplier -> done.accept(() -> supplier.get().flatMap(blocked -> apply(blocked, done)))
              );
            }
            return watermarks;
          }
        }
        return Stream.empty();
      }
      throw new IllegalArgumentException(element.toString());
    }

    @Override
    public int index() {
      return index;
    }

    private Snapshots<Element> snapshots() {
      if (!Snapshots.acking && initializedSnapshots == null) {
        initializedSnapshots = new Snapshots<>(data -> data.id, lastEmitted + 1);
      }
      return initializedSnapshots;
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
      return Hashing.murmur3_32().newHasher(8).putInt(payload.id).putInt(iteration).hash().asInt();
    }
  }

  @SuppressWarnings("Convert2Lambda")
  public static Graph apply(int frontsNumber, List<HashUnit> covering, int iterations, int defaultMinimalTime) {
    final int allIterations = iterations + 1;
    final Graph.Builder graphBuilder = new Graph.Builder();
    final Source source = new Source();
    final Sink sink = new Sink() {
      @Override
      public int index() {
        return allIterations + 2;
      }
    };
    final Graph.Vertex start = new Iteration(1, covering.size(), defaultMinimalTime, null, 0);
    final Graph.Vertex end = new Iteration(
            covering.size(),
            1,
            defaultMinimalTime,
            new HashFunction(covering, allIterations),
            allIterations + 1
    );

    graphBuilder
            .link(source, start)
            .link(
                    IntStream.range(0, allIterations).boxed().<Graph.Vertex>map(iteration ->
                            new Iteration(iteration == 0 ? frontsNumber : covering.size(), covering.size(),
                                    defaultMinimalTime, new HashFunction(covering, iteration), iteration + 1
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
