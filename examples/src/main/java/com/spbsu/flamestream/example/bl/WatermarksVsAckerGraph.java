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
  static final class Child {
    final int parentId, childId;

    Child(int parentId, int childId) {
      this.parentId = parentId;
      this.childId = childId;
    }
  }

  static final class Watermark {
    final int id, partition;

    Watermark(int id, int partition) {
      this.id = id;
      this.partition = partition;
    }

    @Override
    public String toString() {
      return "Watermark(id = " + id + ", partition = " + partition + ")";
    }
  }

  private static Stream<Object> watermarkStream(int parallelism, int id) {
    return IntStream.range(0, parallelism).mapToObj(partition -> new Watermark(
            id,
            partition
    ));
  }

  public static Graph apply(int parallelism, boolean watermarks, int iterations, int childrenNumber) {
    final List<HashUnit> covering = HashUnit.covering(parallelism).collect(Collectors.toCollection(ArrayList::new));

    final Graph.Builder graphBuilder = new Graph.Builder();
    final Source source = new Source();
    final Sink sink = new Sink();
    @SuppressWarnings("Convert2Lambda") Graph.Vertex start = new FlameMap<>(new Function<Integer, Stream<Object>>() {
      @Override
      public Stream<Object> apply(Integer id) {
        Stream<Object> payload = Stream.concat(
                IntStream.range(0, childrenNumber).mapToObj(childId -> new Child(id, childId)),
                Stream.of(id)
        );
        if (!watermarks) {
          return payload;
        }
        return Stream.concat(payload, watermarkStream(parallelism, id));
      }
    }, Integer.class);

    final Graph.Vertex end = new FlameMap<>(
            new Function<Object, Stream<Integer>>() {
              final HashMap<Integer, Integer> counts = new HashMap<>();

              @Override
              public Stream<Integer> apply(Object payload) {
                if (payload instanceof Integer) {
                  if (!watermarks) {
                    return Stream.of((Integer) payload);
                  }
                  return Stream.empty();
                }
                if (payload instanceof Child) {
                  return Stream.empty();
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
                return updated == null ? Stream.of(watermark.id) : Stream.empty();
              }
            },
            Object.class,
            HashFunction.constantHash(0)
    );

    //noinspection Convert2Lambda
    graphBuilder
            .link(source, start)
            .link(IntStream.range(0, iterations).boxed().<Graph.Vertex>map(iteration -> new FlameMap<Object, Object>(
                    new Function<Object, Stream<Object>>() {
                      final HashMap<Integer, Integer> counts = new HashMap<>();

                      @Override
                      public Stream<Object> apply(Object payload) {
                        if (payload instanceof Integer || payload instanceof Child) {
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
                    Object.class,
                    new HashFunction() {
                      @Override
                      public int hash(DataItem dataItem) {
                        Object payload = dataItem.payload(Object.class);
                        final int partition;
                        if (payload instanceof Integer) {
                          partition = Math.floorMod((((Integer) payload) + iteration), parallelism);
                        } else if (payload instanceof Child) {
                          Child child = (Child) payload;
                          partition = Math.floorMod(child.parentId + child.childId + iteration, parallelism);
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
