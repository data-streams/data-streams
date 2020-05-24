package com.spbsu.flamestream.core.data.meta;

import java.util.Arrays;

public class Labels {
  public static final Labels EMPTY = new Labels(0);

  private final Label[] all;

  public Labels(int size) {
    this.all = new Label[size];
  }

  private Labels(Label[] all) {
    this.all = all;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Labels)) {
      return false;
    }
    final Labels labels = (Labels) obj;
    return Arrays.equals(all, labels.all);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(all);
  }

  public Labels added(Label label) {
    final Label[] all = Arrays.copyOf(this.all, label.type + 1);
    assert all[label.type] == null;
    all[label.type] = label;
    return new Labels(all);
  }

  public Label get(int index) {
    if (index < all.length && all[index] != null) {
      return all[index];
    }
    throw new IllegalArgumentException();
  }

  @Override
  public String toString() {
    return Arrays.toString(all);
  }

  public boolean hasAll(LabelsPresence presence) {
    return presence.stream().allMatch(index -> index < all.length && all[index] != null);
  }
}
