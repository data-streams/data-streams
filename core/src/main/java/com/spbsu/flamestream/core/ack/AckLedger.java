package com.spbsu.flamestream.core.ack;

import com.spbsu.flamestream.core.meta.GlobalTime;

public interface AckLedger {
  void report(GlobalTime windowHead, long xor);

  GlobalTime min();

  boolean ack(GlobalTime windowHead, long xor);
}