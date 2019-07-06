package com.spbsu.flamestream.example.bl;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.config.HashUnit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class WatermarksVsAckerGraph {
  static public abstract class Element {
    public final int id;

    protected Element(int id) {this.id = id;}
  }

  static public final class Data extends Element {
    public Data(int id) {
      super(id);
    }
  }
  static public final class Child extends Element {
    final int childId;

    Child(int parentId, int childId) {
      super(parentId);
      this.childId = childId;
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

  public static Graph apply(int parallelism, int iterations, int childrenNumber) {
    final List<HashUnit> covering = HashUnit.covering(parallelism).collect(Collectors.toCollection(ArrayList::new));

    final Graph.Builder graphBuilder = new Graph.Builder();
    final Source source = new Source();
    final Sink sink = new Sink();
    @SuppressWarnings("Convert2Lambda") Graph.Vertex start = new FlameMap<>(new Function<Element, Stream<Element>>() {
      @Override
      public Stream<Element> apply(Element element) {
        if (element instanceof Data) {
          final int id = ((Data) element).id;
          return Stream.concat(
                  IntStream.range(0, childrenNumber).mapToObj(childId -> new Child(id, childId)),
                  Stream.of(element)
          );
        }
        return watermarkStream(parallelism, element.id);
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
                  if (count < parallelism) {
                    return count;
                  }
                  return null;
                });
                return updated == null ? Stream.of(watermark) : Stream.empty();
              }
            },
            Element.class,
            HashFunction.constantHash(0)
    );

    //noinspection Convert2Lambda
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
                          if (count < (iteration == 0 ? 1 : parallelism)) {
                            return count;
                          }
                          return null;
                        });
                        return updated == null ? watermarkStream(
                                iteration + 1 == iterations ? 1 : parallelism,
                                watermark.id
                        ) : Stream.empty();
                      }
                    },
                    Element.class,
                    new HashFunction() {
                      @Override
                      public int hash(DataItem dataItem) {
                        Element payload = dataItem.payload(Element.class);
                        final int partition;
                        if (payload instanceof Data) {
                          partition = Math.floorMod(payload.id + iteration, parallelism);
                        } else if (payload instanceof Child) {
                          Child child = (Child) payload;
                          partition = Math.floorMod(child.id + child.childId + iteration, parallelism);
                        } else {
                          partition = ((Watermark) payload).partition;
                        }
                        return covering.get(partition).from();
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
