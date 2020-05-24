package com.spbsu.flamestream.core.data.meta;

import java.util.Arrays;
import java.util.Objects;

public final class Label {
  public final int type;
  public final GlobalTime globalTime;
  private final int[] childIds;

  public Label(int type, Meta meta) {
    this.type = type;
    globalTime = meta.globalTime();
    childIds = meta.childIds();
  }

  @Override
  public int hashCode() {
    int hashCode = Integer.hashCode(type);
    hashCode = 31 * hashCode + Objects.hashCode(globalTime);
    hashCode = 31 * hashCode + Arrays.hashCode(childIds);
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
            && Objects.equals(globalTime, label.globalTime)
            && Arrays.equals(childIds, label.childIds);
  }

  @Override
  public String toString() {
    return "(" + type + ", " + globalTime + ", " + childIds + ")";
  }
}
