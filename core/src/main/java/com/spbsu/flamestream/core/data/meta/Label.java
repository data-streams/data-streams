package com.spbsu.flamestream.core.data.meta;

import java.util.Objects;

public final class Label<L> {
  public final int index;
  public final L value;
  public final String nodeId;
  public final int uniqueness;
  public final long time;

  public Label(int index, L value, String nodeId, int uniqueness, long time) {
    this.index = index;
    this.value = value;
    this.nodeId = nodeId;
    this.uniqueness = uniqueness;
    this.time = time;
  }

  @Override
  public int hashCode() {
    int hashCode = Integer.hashCode(index);
    hashCode = 31 * hashCode + Objects.hashCode(nodeId);
    hashCode = 31 * hashCode + Integer.hashCode(uniqueness);
    hashCode = 31 * hashCode + Objects.hashCode(value);
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Label)) {
      return false;
    }
    final Label<?> label = (Label<?>) obj;
    return index == label.index
            && Objects.equals(value, label.value)
            && Objects.equals(nodeId, label.nodeId)
            && uniqueness == label.uniqueness;
  }

  @Override
  public String toString() {
    return "(" + index + ", " + value + ", " + nodeId + ", " + uniqueness + ")";
  }
}
