package com.spbsu.flamestream.example.bl.no_barrier;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.example.bl.WatermarksVsAckerGraph;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import com.typesafe.config.ConfigFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;

public class WatermarksVsAckerGraphTest extends FlameAkkaSuite {
  @DataProvider
  public Object[][] dataProvider() {
    return new Object[][]{
            {0},
            {1},
            {10},
            };
  }

  @Test(dataProvider = "dataProvider")
  public void test(Integer iterations) throws InterruptedException, TimeoutException {
    final int parallelism = 4;
    final ActorSystem system = ActorSystem.create("testStand", ConfigFactory.load("remote"));
    try (
            final LocalClusterRuntime runtime = new LocalClusterRuntime(
                    parallelism,
                    new SystemConfig.Builder().millisBetweenCommits(10000).build()
            );
            final FlameRuntime.Flame flame = runtime.run(WatermarksVsAckerGraph.apply(
                    1,
                    HashUnit.covering(parallelism).collect(Collectors.toCollection(ArrayList::new)),
                    iterations
            ))
    ) {
      int streamLength = 100;
      final AwaitResultConsumer<WatermarksVsAckerGraph.Data> awaitResultConsumer =
              new AwaitResultConsumer<>(streamLength);
      CountDownLatch watermarkReceived = new CountDownLatch(parallelism);
      flame.attachRear("Rear", new AkkaRearType<>(system, WatermarksVsAckerGraph.Element.class))
              .forEach(r -> r.addListener(element -> {
                if (element instanceof WatermarksVsAckerGraph.Data) {
                  awaitResultConsumer.accept((WatermarksVsAckerGraph.Data) element);
                }
                if (element instanceof WatermarksVsAckerGraph.Watermark && element.id == -1) {
                  watermarkReceived.countDown();
                }
              }));

      final List<AkkaFront.FrontHandle<Object>> consumers =
              flame.attachFront("Front", new AkkaFrontType<>(system, false))
                      .collect(Collectors.toList());
      for (int i = 1; i < consumers.size(); i++) {
        consumers.get(i).unregister();
      }

      final AkkaFront.FrontHandle<Object> sink = consumers.get(0);
      IntStream.range(-streamLength, 0)
              .boxed()
              .flatMap(id -> Stream.of(
                      new WatermarksVsAckerGraph.Data(id),
                      new WatermarksVsAckerGraph.Watermark(id, 0)
              ))
              .forEach(sink);
      sink.unregister();

      awaitResultConsumer.await(5, TimeUnit.MINUTES);
      watermarkReceived.await();
      assertEquals(awaitResultConsumer.result().count(), awaitResultConsumer.expectedSize);
    }
    Await.ready(system.terminate(), Duration.Inf());
  }
}
