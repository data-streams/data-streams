package com.spbsu.flamestream.core;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import org.jooq.lambda.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Graph {
  Stream<Stream<Vertex>> components();

  Source source();

  Sink sink();

  Stream<Vertex> adjacent(Vertex vertex);

  default TrackingComponent sinkTrackingComponent() {
    for (Vertex vertex : (Iterable<Vertex>) components().flatMap(Function.identity())::iterator)
      if (vertex instanceof Sink)
        return vertex.trackingComponent();
      return null;
  }

  interface Vertex {
    String id();

    default TrackingComponent trackingComponent() { return TrackingComponent.DEFAULT; }

    abstract class Stub implements Vertex {
      private final long id = ThreadLocalRandom.current().nextLong();

      @Override
      public String id() {
        // id shouldn't contain JVM-instance specific fields, e.g. hashCode
        return String.valueOf(id);
      }
    }
  }

  class Builder {
    private final Multimap<Vertex, Vertex> adjLists = LinkedListMultimap.create();
    private final Multimap<Vertex, Vertex> invertedAdjLists = LinkedListMultimap.create();
    private final Set<Set<Vertex>> components = new HashSet<>();

    private final List<Tuple2<Vertex, Vertex>> shuffles = new ArrayList<>();

    public Builder colocate(Vertex... vertices) {
      components.add(new HashSet<>(Arrays.asList(vertices)));
      return this;
    }

    public Builder link(Vertex from, Vertex to) {
      adjLists.put(from, to);
      invertedAdjLists.put(to, from);
      return this;
    }

    public Graph build(Source source, Sink sink) {
      if (invertedAdjLists.keySet().contains(source)) {
        throw new IllegalStateException("Source must not have inputs");
      }
      if (adjLists.keySet().contains(sink)) {
        throw new IllegalStateException("Sink must not have outputs");
      }

      final Set<Vertex> v = new HashSet<>(adjLists.keySet());
      v.addAll(invertedAdjLists.keySet());
      components.forEach(v::removeAll);

      v.forEach(isolated -> components.add(Collections.singleton(isolated)));

      return new MyGraph(adjLists, source, sink, components);
    }

    private static class MyGraph implements Graph {
      private final List<Vertex> allVertices;
      private final Source source;
      private final Sink sink;
      private final Map<Vertex, Collection<Vertex>> adjLists;

      private final Set<Set<Vertex>> components;

      MyGraph(Multimap<Vertex, Vertex> adjLists,
              Source source,
              Sink sink,
              Set<Set<Vertex>> components) {
        this.allVertices = new ArrayList<>(adjLists.keySet());
        this.components = components;
        allVertices.add(sink);
        // Multimap is converted to Map<.,Set> to support serialization
        this.adjLists = adjLists.entries().stream().collect(Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.mapping(
                        Map.Entry::getValue,
                        Collectors.toCollection(LinkedHashSet::new)
                )
        ));

        this.source = source;
        this.sink = sink;
      }

      @Override
      public Stream<Vertex> adjacent(Vertex vertex) {
        return adjLists.getOrDefault(vertex, Collections.emptyList()).stream();
      }

      @Override
      public Source source() {
        return source;
      }

      @Override
      public Sink sink() {
        return sink;
      }

      @Override
      public Stream<Stream<Vertex>> components() {
        return components.stream().map(Collection::stream);
      }
    }
  }
}