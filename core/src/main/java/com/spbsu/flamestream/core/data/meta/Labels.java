package com.spbsu.flamestream.core.data.meta;

import java.util.Arrays;

public class Labels {
  public static final Labels EMPTY = new Labels(0);

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
    assert all[label.index] == null;
    final Label<?>[] byType = this.all.clone();
    byType[label.index] = label;
    return new Labels(byType);
  }

  public Label<?> get(int index) {
    return all[index];
  }

  public boolean hasAll(LabelsPresence presence) {
    return presence.stream().allMatch(index -> all[index] != null);
  }
}
