package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorContext;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.Comparator;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public abstract class Joba {
  public static class Id {
    final String nodeId;
    final String vertexId;
    private static final Comparator<Id> comparator = Comparator
            .<Id, String>comparing(id -> id.nodeId)
            .thenComparing(id -> id.vertexId);

    public Id(String nodeId, String vertexId) {
      this.nodeId = nodeId;
      this.vertexId = vertexId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Id that = (Id) o;
      return Objects.equals(nodeId, that.nodeId) &&
              Objects.equals(vertexId, that.vertexId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(nodeId, vertexId);
    }
  }

  final Id id;

  Joba(Id id) {
    this.id = id;
  }

  abstract void accept(DataItem item,
                       Consumer<DataItem> sink,
                       int vertexIndex,
                       Consumer<Supplier<Stream<DataItem>>> supplierConsumer);

  void onMinTime(GlobalTime time) {
  }

  void onPrepareCommit(GlobalTime time) {
  }
}
