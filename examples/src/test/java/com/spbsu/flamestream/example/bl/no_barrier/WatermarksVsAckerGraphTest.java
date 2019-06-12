package com.spbsu.flamestream.example.bl.no_barrier;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.example.bl.WatermarksVsAckerGraph;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.WorkerApplication;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import com.typesafe.config.ConfigFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;

public class WatermarksVsAckerGraphTest extends FlameAkkaSuite {
  @DataProvider
  public Object[][] dataProvider() {
    return new Object[][]{
            {false, 0, 0},
            {false, 1, 0},
            {false, 10, 0},
            {true, 0, 0},
            {true, 1, 0},
            {true, 10, 0},
            {false, 0, 10},
            {false, 1, 10},
            {false, 10, 10},
            {true, 0, 10},
            {true, 1, 10},
            {true, 10, 10}
    };
  }

  @Test(dataProvider = "dataProvider")
  public void test(Boolean watermarks, Integer iterations, Integer childrenNumber) throws
                                                                                   InterruptedException,
                                                                                   TimeoutException {
    final int parallelism = 4;
    final ActorSystem system = ActorSystem.create("testStand", ConfigFactory.load("remote"));
    try (
            final LocalClusterRuntime runtime = new LocalClusterRuntime(
                    parallelism,
                    new WorkerApplication.WorkerConfig.Builder()
                            .millisBetweenCommits(10000).barrierDisabled(watermarks)::build
            );
            final FlameRuntime.Flame flame = runtime.run(WatermarksVsAckerGraph.apply(
                    parallelism,
                    watermarks,
                    iterations,
                    childrenNumber
            ))
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
      IntStream.range(-streamLength, 0).boxed().forEach(sink);
      sink.unregister();

      awaitConsumer.await(5, TimeUnit.MINUTES);
      assertEquals(streamLength, awaitConsumer.result().count());
    }
    Await.ready(system.terminate(), Duration.Inf());
  }
}
