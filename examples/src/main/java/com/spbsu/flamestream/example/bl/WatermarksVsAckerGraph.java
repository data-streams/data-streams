package com.spbsu.flamestream.example.bl;

import com.google.common.hash.Hashing;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.config.HashUnit;

import java.util.HashMap;
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
    private final int partition;

    public Watermark(int id) {
      this(id, 0);
    }

    public Watermark(int id, int partition) {
      super(id);
      this.partition = partition;
    }

    @Override
    public String toString() {
      return "Watermark(id = " + id + ", partition = " + partition + ")";
    }
  }

  private static Stream<Element> watermarkStream(int parallelism, int id) {
    return IntStream.range(0, parallelism).mapToObj(partition -> new Watermark(
            id,
            partition
    ));
  }

  @SuppressWarnings("Convert2Lambda")
  public static Graph apply(final List<HashUnit> covering, int iterations, int childrenNumber) {
    final Graph.Builder graphBuilder = new Graph.Builder();
    final Source source = new Source();
    final Sink sink = new Sink();
    Graph.Vertex start = new FlameMap<>(new Function<Element, Stream<Element>>() {
      @Override
      public Stream<Element> apply(Element element) {
        if (element instanceof Data) {
          final int id = ((Data) element).id;
          return Stream.concat(
                  IntStream.range(0, childrenNumber).mapToObj(childId -> new Child(id, childId)),
                  Stream.of(element)
          );
        }
        return watermarkStream(covering.size(), element.id);
      }
    }, Element.class);

    final Graph.Vertex end = new FlameMap<>(
            new Function<Element, Stream<Element>>() {
              final HashMap<Integer, Integer> counts = new HashMap<>();

              @Override
              public Stream<Element> apply(Element payload) {
                if (payload instanceof Data || payload instanceof Child) {
                  return Stream.of(payload);
                }
                final Watermark watermark = (Watermark) payload;
                Integer updated = counts.compute(watermark.id, (ignored, count) -> {
                  if (count == null) {
                    count = 0;
                  }
                  count++;
                  if (count < covering.size()) {
                    return count;
                  }
                  return null;
                });
                return updated == null ? Stream.of(watermark) : Stream.empty();
              }
            },
            Element.class,
            HashFunction.uniformHash(new HashFunction() {
              @Override
              public int hash(DataItem dataItem) {
                return dataItem.payload(Element.class).id;
              }
            })
    );

    graphBuilder
            .link(source, start)
            .link(IntStream.range(0, iterations).boxed().<Graph.Vertex>map(iteration -> new FlameMap<>(
                    new Function<Element, Stream<Element>>() {
                      final HashMap<Integer, Integer> counts = new HashMap<>();

                      @Override
                      public Stream<Element> apply(Element payload) {
                        if (payload instanceof Data || payload instanceof Child) {
                          return Stream.of(payload);
                        }
                        final Watermark watermark = (Watermark) payload;
                        Integer updated = counts.compute(watermark.id, (ignored, count) -> {
                          if (count == null) {
                            count = 0;
                          }
                          count++;
                          if (count < (iteration == 0 ? 1 : covering.size())) {
                            return count;
                          }
                          return null;
                        });
                        return updated == null ? watermarkStream(
                                iteration + 1 == iterations ? 1 : covering.size(),
                                watermark.id
                        ) : Stream.empty();
                      }
                    },
                    Element.class,
                    new HashFunction() {
                      @Override
                      public int hash(DataItem dataItem) {
                        Element payload = dataItem.payload(Element.class);
                        if (payload instanceof Watermark) {
                          return covering.get(((Watermark) payload).partition).from();
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
            )).reduce(start, (from, to) -> {
              graphBuilder.link(from, to);
              return to;
            }), end)
            .link(end, sink);
    return graphBuilder.build(source, sink);
  }
}
