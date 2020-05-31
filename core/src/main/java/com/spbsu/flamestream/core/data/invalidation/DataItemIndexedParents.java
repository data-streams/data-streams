package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.TreeMap;
import java.util.function.Consumer;

public interface DataItemIndexedParents {
  void add(DataItem dataItem);

  void remove(DataItem dataItem);

  void forEachParent(DataItem dataItem, Consumer<DataItem> consumer);

  class TreeImpl implements DataItemIndexedParents {
    private static class Key implements Comparable<Key> {
      static final Comparator<Key> COMPARATOR =
              Comparator.<Key, GlobalTime>comparing(key -> key.meta.globalTime())
                      .thenComparing((key1, key2) -> key1.meta.comparePrefixedChildIds(
                              key1.childIdsPrefix,
                              key2.meta,
                              key2.childIdsPrefix
                      )).thenComparingLong(key -> key.trace);

      final Meta meta;
      final int childIdsPrefix;
      final long trace;

      private Key(Meta meta) {this(meta, meta.childIdsLength(), meta.trace());}

      private Key(Meta meta, int childIdsPrefix, long trace) {
        this.meta = meta;
        this.childIdsPrefix = childIdsPrefix;
        this.trace = trace;
      }

      @Override
      public int compareTo(@NotNull Key that) {
        return COMPARATOR.compare(this, that);
      }
    }

    private final TreeMap<Key, DataItem> tree = new TreeMap<>();

    @Override
    public void add(DataItem dataItem) {
      if (tree.put(new Key(dataItem.meta()), dataItem) != null) {
        throw new RuntimeException();
      }
    }

    @Override
    public void remove(DataItem dataItem) {
      if (tree.remove(new Key(dataItem.meta())) == null) {
        throw new RuntimeException();
      }
    }

    @Override
    public void forEachParent(DataItem dataItem, Consumer<DataItem> consumer) {
      for (int prefix = 0; prefix < dataItem.meta().childIdsLength(); prefix++) {
        tree.subMap(
                new Key(dataItem.meta(), prefix, Long.MIN_VALUE),
                true,
                new Key(dataItem.meta(), prefix, Long.MAX_VALUE),
                true
        ).values().forEach(consumer);
      }
    }
  }
}
