package com.spbsu.flamestream.example.bl;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.WorkerApplication;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import com.typesafe.config.ConfigFactory;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;

public class AckerGraphTest {
  @Test(invocationCount = 5)
  public void test() throws InterruptedException, TimeoutException {
    final int parallelism = 4;
    final ActorSystem system = ActorSystem.create("testStand", ConfigFactory.load("remote"));
    try (
            final LocalClusterRuntime runtime = new LocalClusterRuntime(
                    parallelism,
                    new WorkerApplication.WorkerConfig.Builder().millisBetweenCommits(10000)::build
            );
            final FlameRuntime.Flame flame = runtime.run(AckerGraph.apply(4))
    ) {
      int streamLength = 100;
      final AwaitResultConsumer<Integer> awaitConsumer = new AwaitResultConsumer<>(streamLength);
      flame.attachRear("Rear", new AkkaRearType<>(system, Integer.class))
              .forEach(r -> r.addListener(awaitConsumer));

      final List<AkkaFront.FrontHandle<Object>> consumers =
              flame.attachFront("Front", new AkkaFrontType<>(system, false))
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
    Await.ready(system.terminate(), Duration.Inf());
  }
}
