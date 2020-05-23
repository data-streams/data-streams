package com.spbsu.flamestream.core.data.meta;

import java.util.Objects;

public final class Label {
  public final int type;
  public final String nodeId;
  public final long time;
  public final int uniqueness;

  public Label(int type, String nodeId, long time, int uniqueness) {
    this.type = type;
    this.nodeId = nodeId;
    this.time = time;
    this.uniqueness = uniqueness;
  }

  @Override
  public int hashCode() {
    int hashCode = Integer.hashCode(type);
    hashCode = 31 * hashCode + Objects.hashCode(nodeId);
    hashCode = 31 * hashCode + Long.hashCode(time);
    hashCode = 31 * hashCode + Integer.hashCode(uniqueness);
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
    final Label label = (Label) obj;
    return type == label.type
            && Objects.equals(nodeId, label.nodeId)
            && time == label.time
            && uniqueness == label.uniqueness;
  }

  @Override
  public String toString() {
    return "(" + type + ", " + nodeId + ", " + time + ", " + uniqueness + ")";
  }
}
