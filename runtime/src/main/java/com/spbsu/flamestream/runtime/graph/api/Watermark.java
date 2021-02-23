package com.spbsu.flamestream.runtime.graph.api;

import com.spbsu.flamestream.runtime.graph.GraphManager;
import com.spbsu.flamestream.runtime.graph.Joba;

public class Watermark {
  public final Joba.Id from;
  public final GraphManager.Destination to;
  public final long time;

  public Watermark(Joba.Id from, GraphManager.Destination to, long time) {
    this.from = from;
    this.to = to;
    this.time = time;
  }

  @Override
  public String toString() {
    return "Watermark(" + from + ", " + to + ", " + time + ")";
  }
}
