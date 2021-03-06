package com.spbsu.flamestream.runtime.acceptance;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;

import java.util.List;
import java.util.Queue;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 22.12.2017
 */
public class FlameAkkaSuite extends FlameStreamSuite {
  protected static int DEFAULT_PARALLELISM = 4;

  protected <T> void applyDataToHandleAsync(Stream<T> data, AkkaFront.FrontHandle<T> handle) {
    final Thread thread = new Thread(() -> {
      data.forEach(handle);
      handle.unregister();
    });
    thread.setDaemon(true);
    thread.start();
  }

  protected <T> void applyDataToAllHandlesAsync(Queue<T> data, List<AkkaFront.FrontHandle<T>> handles) {
    IntStream.range(0, handles.size()).forEach(i -> {
      final Thread thread = new Thread(() -> {
        final AkkaFront.FrontHandle<T> handle = handles.get(i);
        while (true) {
          final T item = data.poll();
          if (item != null) {
            handle.accept(item);
          } else {
            handle.unregister();
            break;
          }
        }
      });
      thread.setDaemon(true);
      thread.start();
    });
  }
}
