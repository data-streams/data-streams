package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.SerializableComparator;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * User: Artem
 * Date: 01.11.2017
 */
public class ArrayInvalidatingBucket implements InvalidatingBucket {
  private final List<DataItem> innerList;
  private final SerializableComparator<DataItem> customOrder;
  private final SerializableComparator<DataItem> order;
  private final DataItemIndexedParents dataItemIndexedParents;

  public ArrayInvalidatingBucket() {
    this((first, second) -> 0);
  }

  public ArrayInvalidatingBucket(SerializableComparator<DataItem> customOrder) {
    this(new ArrayList<>(), new DataItemIndexedParents.TreeImpl(), customOrder);
  }

  private ArrayInvalidatingBucket(
          List<DataItem> innerList,
          DataItemIndexedParents dataItemIndexedParents,
          SerializableComparator<DataItem> customOrder
  ) {
    this.innerList = innerList;
    this.customOrder = customOrder;
    this.order = SerializableComparator.comparing((DataItem dataItem) -> dataItem.meta().globalTime())
            .thenComparing(customOrder)
            .thenComparing(DataItem::meta);
    this.dataItemIndexedParents = dataItemIndexedParents;
  }

  @Override
  public void insert(DataItem insertee) {
    if (!insertee.meta().isTombstone()) {
      dataItemIndexedParents.forEachParent(insertee, parent -> {
        if (order.compare(insertee, parent) <= 0) {
          throw new RuntimeException();
        }
      });
      dataItemIndexedParents.add(insertee);
      final int position = insertionPosition(insertee);
      innerList.add(position, insertee);
    } else {
      final int position = insertionPosition(insertee) - 1;
      if (!innerList.get(position).meta().isInvalidedBy(insertee.meta())) {
        throw new IllegalStateException("There is no invalidee");
      }
      innerList.remove(position);
      dataItemIndexedParents.remove(insertee);
    }
  }

  @Override
  public DataItem get(int index) {
    return innerList.get(index);
  }

  @Override
  public void forRange(int fromIndex, int toIndex, Consumer<DataItem> consumer) {
    for (int i = fromIndex; i < toIndex; i++) {
      consumer.accept(innerList.get(i));
    }
  }

  @Override
  public void clearRange(int fromIndex, int toIndex) {
    final List<DataItem> dataItems = innerList.subList(fromIndex, toIndex);
    dataItems.forEach(dataItemIndexedParents::remove);
    dataItems.clear();
  }

  @Override
  public int size() {
    return innerList.size();
  }

  @Override
  public boolean isEmpty() {
    return innerList.isEmpty();
  }

  @Override
  public int insertionPosition(DataItem dataItem) {
    return lowerBound(listDataItem -> order.compare(listDataItem, dataItem));
  }

  @Override
  public int lowerBound(GlobalTime globalTime) {
    return lowerBound(dataItem -> dataItem.meta().globalTime().compareTo(globalTime));
  }

  @Override
  public InvalidatingBucket subBucket(GlobalTime globalTime, int window) {
    final ArrayList<DataItem> innerList = subList(globalTime, window);
    final DataItemIndexedParents.TreeImpl tree = new DataItemIndexedParents.TreeImpl();
    innerList.forEach(tree::add);
    return new ArrayInvalidatingBucket(innerList, tree, customOrder);
  }

  @NotNull
  private ArrayList<DataItem> subList(GlobalTime globalTime, int window) {
    final int start = lowerBound(globalTime);
    return new ArrayList<>(innerList.subList(Math.max(start - window + 1, 0), start));
  }

  /**
   * Lower bound as Burunduk1 says
   * <a href="http://acm.math.spbu.ru/~sk1/mm/cs-center/src/2015-09-24/bs.cpp.html"/>"/>
   */
  private int lowerBound(ToIntFunction<DataItem> comparator) {
    int left = 0;
    int right = innerList.size();
    while (left != right) {
      final int middle = left + (right - left) / 2;
      if (comparator.applyAsInt(innerList.get(middle)) >= 0) {
        right = middle;
      } else {
        left = middle + 1;
      }
    }
    return left;
  }
}
