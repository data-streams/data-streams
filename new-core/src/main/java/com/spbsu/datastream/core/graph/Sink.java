package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Created by marnikitta on 2/6/17.
 */
public abstract class Sink<T> implements AtomicGraph {
  private final InPort inPort = new InPort();

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.emptyList();
  }

  public InPort inPort() {
    return inPort;
  }
}