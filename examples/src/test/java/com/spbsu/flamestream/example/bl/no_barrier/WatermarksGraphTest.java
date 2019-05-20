package com.spbsu.flamestream.example.bl.no_barrier;

import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;

public class WatermarksGraphTest extends FlameAkkaSuite {
  @Test(invocationCount = 5)
  public void test() throws InterruptedException {
    final int parallelism = 4;
    try (final LocalRuntime runtime = new LocalRuntime.Builder().parallelism(parallelism).build()) {
      try (final FlameRuntime.Flame flame = runtime.run(WatermarksGraph.apply(parallelism, 4))) {
        int streamLength = 100;
        final AwaitResultConsumer<Integer> awaitConsumer = new AwaitResultConsumer<>(streamLength);
        flame.attachRear("Rear", new AkkaRearType<>(runtime.system(), Integer.class))
                .forEach(r -> r.addListener(awaitConsumer));

        final List<AkkaFront.FrontHandle<Object>> consumers =
                flame.attachFront("Front", new AkkaFrontType<>(runtime.system(), false))
                        .collect(Collectors.toList());
        for (int i = 1; i < consumers.size(); i++) {
          consumers.get(i).unregister();
        }

        final AkkaFront.FrontHandle<Object> sink = consumers.get(0);
        IntStream.range(0, streamLength).boxed().forEach(sink);
        sink.unregister();

        awaitConsumer.await(5, TimeUnit.MINUTES);
        assertEquals(streamLength, awaitConsumer.result().count());
      }
    }
  }
}
