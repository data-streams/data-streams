package com.spbsu.flamestream.runtime.config;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public class Snapshots<Element> {
  public static final boolean acking =
          System.getenv().containsKey("ACKERS_NUMBER") ? parseInt(System.getenv("ACKERS_NUMBER")) > 0 : true;

  public static void flush() throws Exception {
    try (final PrintWriter printWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(
            "/tmp/snapshots.csv"
    )))) {
      printWriter.println("buffering_duration," + totalBufferingDuration.get());
      printWriter.println("buffered_count," + totalBufferedCount.get());
    }
  }

  private static final int window =
          System.getenv().containsKey("SNAPSHOTS_WINDOW") ? parseInt(System.getenv("SNAPSHOTS_WINDOW")) : 0;
  private static final long durationMs =
          System.getenv().containsKey("SNAPSHOTS_DURATION_MS") ? parseLong(System.getenv("SNAPSHOTS_DURATION_MS")) : 0;
  private static final long baseNanos = System.nanoTime();
  private static final AtomicLong totalBufferingDuration = new AtomicLong();
  private static final AtomicInteger totalBufferedCount = new AtomicInteger();

  public Snapshots(ToLongFunction<Element> elementTime, long defaultMinimalTime) {
    this.elementTime = elementTime;
    this.defaultMinimalTime = defaultMinimalTime;
    this.minTime = defaultMinimalTime;
    buffer = new PriorityQueue<>(Comparator.comparingLong(this.elementTime));
  }

  private final ToLongFunction<Element> elementTime;
  private final long defaultMinimalTime;
  private final PriorityQueue<Element> buffer;
  private long minTime;
  private long bufferingDuration = 0;
  private int bufferedCount = 0;

  public boolean putIfBlocked(Element element) {
    if (blocked(element)) {
      buffer.add(element);
      bufferedCount++;
      bufferingDuration -= System.nanoTime() - baseNanos;
      return true;
    }
    return false;
  }

  public List<Element> minTimeUpdate(long time) {
    for (int i = 0; i < period(time) - period(minTime); i++) {
      try {
        System.out.println("SNAPSHOT");
        Thread.sleep(durationMs);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    minTime = time;
    List<Element> elements = new ArrayList<>();
    while (!buffer.isEmpty() && !blocked(buffer.peek())) {
      bufferingDuration += System.nanoTime() - baseNanos;
      elements.add(buffer.poll());
    }
    if (buffer.isEmpty()) {
      totalBufferingDuration.addAndGet(bufferingDuration);
      bufferingDuration = 0;
      totalBufferedCount.addAndGet(bufferedCount);
      bufferedCount = 0;
    }
    return elements;
  }

  private long period(long time) {
    if (window > 0) {
      return Math.floorDiv(time - defaultMinimalTime, window);
    }
    return 0;
  }

  public boolean blocked(Element item) {
    return period(minTime) < period(elementTime.applyAsLong(item));
  }

  public boolean blocked(long time) {
    return period(minTime) < period(time);
  }
}
