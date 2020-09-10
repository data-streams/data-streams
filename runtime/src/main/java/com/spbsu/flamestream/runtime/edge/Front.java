package com.spbsu.flamestream.runtime.edge;

import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 15.11.2017
 */
public interface Front {
  void onStart(Consumer<Object> consumer, GlobalTime from);

  void onRequestNext();

  void onCheckpoint(GlobalTime to);

  abstract class Stub implements Front {
    protected final EdgeId edgeId;
    private long prevGlobalTs = 0;

    protected Stub(EdgeId edgeId) {
      this.edgeId = edgeId;
    }

    protected synchronized GlobalTime currentTime() {
      return new GlobalTime(prevGlobalTs++, edgeId);
    }
  }
}
