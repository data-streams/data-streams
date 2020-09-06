package com.spbsu.flamestream.example.bl;

import com.google.common.hash.Hashing;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.TrackingComponent;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.HashUnit;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
      return "Watermark(id = " + id + ", fromPartition = " + fromPartition + ", toPartition = " + toPartition + ")";
    }
  }

  private static Stream<Element> watermarkStream(int parallelism, int id, int fromPartition) {
    return IntStream.range(0, parallelism).mapToObj(toPartition -> new Watermark(
            id,
            fromPartition,
            toPartition
    ));
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

  static private Graph.Vertex iteration(
          int fromPartitions,
          int toPartitions,
          int defaultMinimalTime,
          HashFunction hashFunction
  ) {
    final int[] watermarks = new int[fromPartitions];
    Arrays.fill(watermarks, defaultMinimalTime);
    return new FlameMap.Builder<Element, Element>(new SerializableFunction<>() {
      int lastEmitted = defaultMinimalTime;

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
    }, Element.class).hashFunction(hashFunction).build();
  }

  @SuppressWarnings("Convert2Lambda")
  public static Graph apply(int frontsNumber, List<HashUnit> covering, int iterations, int defaultMinimalTime) {
    final int allIterations = iterations + 1;
    final Graph.Builder graphBuilder = new Graph.Builder();

    final Source source = new Source();
    final HashMap<Graph.Vertex, TrackingComponent> vertexTrackingComponent = new HashMap<>();
    TrackingComponent currentTrackingComponent = new TrackingComponent(0, Collections.emptySet());
    vertexTrackingComponent.put(source, currentTrackingComponent);

    Graph.Vertex prev = iteration(1, covering.size(), defaultMinimalTime, null);
    currentTrackingComponent = new TrackingComponent(
            currentTrackingComponent.index + 1,
            Collections.singleton(currentTrackingComponent)
    );
    vertexTrackingComponent.put(prev, currentTrackingComponent);
    graphBuilder.link(source, prev);

    for (int iteration = 0; iteration < allIterations; iteration++) {
      final Graph.Vertex next = iteration(
              iteration == 0 ? frontsNumber : covering.size(),
              covering.size(),
              defaultMinimalTime,
              new HashFunction(covering, iteration)
      );
      currentTrackingComponent = new TrackingComponent(
              currentTrackingComponent.index + 1,
              Collections.singleton(currentTrackingComponent)
      );
      vertexTrackingComponent.put(next, currentTrackingComponent);
      graphBuilder.link(prev, next);
      prev = next;
    }

    final Graph.Vertex end = iteration(
            covering.size(),
            1,
            defaultMinimalTime,
            new HashFunction(covering, allIterations)
    );
    currentTrackingComponent = new TrackingComponent(
            currentTrackingComponent.index + 1,
            Collections.singleton(currentTrackingComponent)
    );
    vertexTrackingComponent.put(end, currentTrackingComponent);
    graphBuilder.link(prev, end);

    final Sink sink = new Sink();
    currentTrackingComponent = new TrackingComponent(
            currentTrackingComponent.index + 1,
            Collections.singleton(currentTrackingComponent)
    );
    vertexTrackingComponent.put(sink, currentTrackingComponent);
    graphBuilder.link(end, sink);

    return graphBuilder.vertexTrackingComponent(vertexTrackingComponent::get).build(source, sink);
  }
}
