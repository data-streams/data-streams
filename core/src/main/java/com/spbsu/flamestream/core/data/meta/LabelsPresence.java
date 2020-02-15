package com.spbsu.flamestream.core.data.meta;

import org.jetbrains.annotations.NotNull;

import java.util.stream.IntStream;

public class LabelsPresence {
  public static LabelsPresence EMPTY = new LabelsPresence(0);

  private final boolean[] all;

  public LabelsPresence(int size) {
    this.all = new boolean[size];
  }

  @NotNull
  public IntStream stream() {
    return IntStream.range(0, all.length).filter(index -> all[index]);
  }
}
