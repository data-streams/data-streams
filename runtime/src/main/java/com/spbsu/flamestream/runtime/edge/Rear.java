package com.spbsu.flamestream.runtime.edge;

import com.spbsu.flamestream.core.Batch;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface Rear {
  class MinTime {
    public final GlobalTime time;

    public MinTime(GlobalTime time) {this.time = time;}

    @Override
    public String toString() {
      return time.toString();
    }
  }

  /**
   * Sync call, return mean accept
   */
  void accept(Batch batch);

  Batch last();
}
