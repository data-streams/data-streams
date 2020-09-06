package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.runtime.edge.Front;
import com.spbsu.flamestream.runtime.FlameRuntime;

public class AttachFront<F extends Front> {
  private final String id;
  private final FlameRuntime.FrontInstance<F> instance;
  public final int trackingWindow;

  public AttachFront(String id, FlameRuntime.FrontInstance<F> instance, int trackingWindow) {
    this.id = id;
    this.instance = instance;
    this.trackingWindow = trackingWindow;
  }

  public String id() {
    return id;
  }

  public FlameRuntime.FrontInstance<F> instance() {
    return instance;
  }
}
