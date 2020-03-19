package com.spbsu.flamestream.runtime.edge;

import com.spbsu.flamestream.core.Batch;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.concurrent.CompletionStage;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface Rear {
  class MinTime {
    public final GlobalTime time;

    public MinTime(GlobalTime time) {this.time = time;}
  }

  /**
   * Sync call, completion means accept
   */
  CompletionStage<?> accept(Batch batch);

  Batch last();
}
