package com.spbsu.flamestream.runtime.master.acker.table;

public final class ArrayAckTable implements AckTable {
  private final long[] xors;
  private final int window;

  private int headPosition;
  private long headValue;
  private final boolean assertAckingBackInTime;

  public ArrayAckTable(long headValue, int capacity, int window, boolean assertAckingBackInTime) {
    this.window = window;
    this.xors = new long[capacity];
    this.headValue = headValue;
    this.headPosition = 0;
    this.assertAckingBackInTime = assertAckingBackInTime;
  }

  @Override
  public boolean ack(long timestamp, long xor) {
    int headOffset = Math.floorDiv(Math.toIntExact(timestamp - headValue), window);
    if (headOffset < 0 && assertAckingBackInTime)
      throw new RuntimeException("acking back in time: headValue = " + headValue + ", timestamp = " + timestamp);
    while (headOffset < 0) {
      final int newHeadPosition = (headPosition - 1 + xors.length) % xors.length;
      if (xors[newHeadPosition] != 0) {
        throw new IllegalArgumentException("Ring buffer overflow");
      }
      headPosition = newHeadPosition;
      headValue -= window;
      headOffset++;
    }
    if (headOffset > xors.length) {
      throw new IllegalArgumentException("Ring buffer overflow");
    } else {
      xors[(headPosition + headOffset) % xors.length] ^= xor;
      return xors[(headPosition + headOffset) % xors.length] == 0;
    }
  }

  @Override
  public long tryPromote(long upTo) {
    int steps = 0;
    while (xors[headPosition] == 0 && headValue <= (upTo - window)) {
      headPosition = (headPosition + 1) % xors.length;
      headValue += window;
      steps++;

      //Optimize the case, when table is empty and upTo == Long.MAX_VALUE.
      if (steps == xors.length) {
        headValue = Math.max(upTo, headValue);
        headPosition = 0;
      }
    }
    return headValue;
  }
}
