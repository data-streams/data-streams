package com.spbsu.flamestream.core.data.meta;

import java.util.Arrays;

public class Labels {
  private final Label<?>[] all;

  public Labels(int size) {
    this.all = new Label[size];
  }

  private Labels(Label<?>[] all) {
    this.all = all;
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj.equals(all);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(all);
  }

  public Labels added(Label<?> label) {
    final Label<?>[] all = Arrays.copyOf(this.all, label.index + 1);
    assert all[label.index] == null;
    all[label.index] = label;
    return new Labels(all);
  }

  public Label<?> get(int index) {
    if (index < all.length && all[index] != null) {
      return all[index];
    }
    throw new IllegalArgumentException();
  }

  public boolean hasAll(LabelsPresence presence) {
    return presence.stream().allMatch(index -> index < all.length && all[index] != null);
  }
}
