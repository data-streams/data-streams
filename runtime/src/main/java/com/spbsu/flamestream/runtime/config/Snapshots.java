package com.spbsu.flamestream.runtime.config;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public class Snapshots<Element> {
  public static final boolean acking =
          System.getenv().containsKey("ACKERS_NUMBER") ? parseInt(System.getenv("ACKERS_NUMBER")) > 0 : true;
  private static final int verticesNumber =
          System.getenv().containsKey("ACKER_VERTICES_NUMBER") ? parseInt(System.getenv("ACKER_VERTICES_NUMBER")) :
                  100;

  public static void flush() throws Exception {
    long totalBufferingDuration = 0;
    int totalBufferedCount = 0;
    try (final PrintWriter printWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(
            "/tmp/vertex_snapshots.csv"
    )))) {
      int nonZeroLength = 0;
      for (int i = 0; i < verticesNumber; i++) {
        totalBufferingDuration += vertexTotalBufferingDuration.get(i);
        totalBufferedCount += vertexTotalBufferedCount.get(i);
        if (vertexTotalBufferingDuration.get(i) > 0 || vertexTotalBufferedCount.get(i) > 0)
          nonZeroLength = i + 1;
      }
      printWriter.println("buffering_duration,buffered_count");
      for (int i = 0; i < nonZeroLength; i++) {
        printWriter.println(vertexTotalBufferingDuration.get(i) + "," + vertexTotalBufferedCount.get(i));
      }
    }
    try (final PrintWriter printWriter = new PrintWriter(Files.newBufferedWriter(Paths.get(
            "/tmp/snapshots.csv"
    )))) {
      printWriter.println("buffering_duration," + totalBufferingDuration);
      printWriter.println("buffered_count," + totalBufferedCount);
    }
  }

  private static final int window =
          System.getenv().containsKey("SNAPSHOTS_WINDOW") ? parseInt(System.getenv("SNAPSHOTS_WINDOW")) : 0;
  public static final long durationMs =
          System.getenv().containsKey("SNAPSHOTS_DURATION_MS") ? parseLong(System.getenv("SNAPSHOTS_DURATION_MS")) : 0;
  private static final long baseNanos = System.nanoTime();
  private static final AtomicLongArray vertexTotalBufferingDuration = new AtomicLongArray(verticesNumber);
  private static final AtomicIntegerArray vertexTotalBufferedCount = new AtomicIntegerArray(verticesNumber);

  public Snapshots(ToLongFunction<Element> elementTime, long defaultMinimalTime, int vertexIndex) {
    this.elementTime = elementTime;
    this.defaultMinimalTime = defaultMinimalTime;
    scheduledPeriod = currentPeriod = period(defaultMinimalTime);
    this.vertexIndex = vertexIndex;
    buffer = new PriorityQueue<>(Comparator.comparingLong(this.elementTime));
  }

  private final ToLongFunction<Element> elementTime;
  private final long defaultMinimalTime;
  private final PriorityQueue<Element> buffer;
  private long scheduledPeriod;
  private final int vertexIndex;
  private long currentPeriod;
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

  public void minTimeUpdate(long time, Consumer<Supplier<Stream<Element>>> scheduleDone) {
    checkPeriodIncrement(time, scheduleDone);
  }

  private void checkPeriodIncrement(long time, Consumer<Supplier<Stream<Element>>> scheduleSnapshotDone) {
    if (period(time) > scheduledPeriod) {
      System.out.println("SNAPSHOT");
      scheduledPeriod++;
      scheduleSnapshotDone.accept(() -> {
        currentPeriod++;
        checkPeriodIncrement(time, scheduleSnapshotDone);
        List<Element> elements = new ArrayList<>();
        while (!buffer.isEmpty() && !blocked(buffer.peek())) {
          bufferingDuration += System.nanoTime() - baseNanos;
          elements.add(buffer.poll());
        }
        if (buffer.isEmpty()) {
          vertexTotalBufferingDuration.addAndGet(vertexIndex, bufferingDuration);
          bufferingDuration = 0;
          vertexTotalBufferedCount.addAndGet(vertexIndex, bufferedCount);
          bufferedCount = 0;
        }
        return elements.stream();
      });
    }
  }

  private long period(long time) {
    if (window > 0) {
      return Math.floorDiv(time - defaultMinimalTime, window);
    }
    return 0;
  }

  public boolean blocked(Element item) {
    return blocked(elementTime.applyAsLong(item));
  }

  public boolean blocked(long time) {
    return currentPeriod < period(time);
  }
}
