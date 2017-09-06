package com.spbsu.datastream.core.stat;

import java.util.HashMap;
import java.util.Map;

public final class GroupingStatistics implements Statistics {

  private long cumulativeReplay = 0;
  private long replaySamples = 0;

  public void recordReplaySize(int replaySize) {
    cumulativeReplay += replaySize;
    replaySamples++;
  }

  private long bucketSize = 0;
  private int bucketSizeSamples = 0;

  public void recordBucketSize(long size) {
    bucketSize += size;
    bucketSizeSamples++;
  }

  @Override
  public Map<String, Double> metrics() {
    final Map<String, Double> result = new HashMap<>();
    result.put("Average replay size", (double) cumulativeReplay / replaySamples);
    result.put("Average bucket size", (double) bucketSize / bucketSizeSamples);
    return result;
  }

  @Override
  public String toString() {
    return metrics().toString();
  }
}
