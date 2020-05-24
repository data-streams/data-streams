package com.spbu.flamestream.client;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Job;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Label;
import com.spbsu.flamestream.core.data.meta.Labels;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.Rear;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ZkFlameClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(ZkFlameClientTest.class);

  @DataProvider
  public static Object[][] dataProvider() {
    return new Object[][]{
            {SystemConfig.WorkersResourcesDistributor.DEFAULT_DISABLED, true},
            {SystemConfig.WorkersResourcesDistributor.DEFAULT_CENTRALIZED, false},
            {SystemConfig.WorkersResourcesDistributor.DEFAULT_DISTRIBUTED, false},
            };
  }

  @Test(dataProvider = "dataProvider")
  public void testPushJobWorks(
          SystemConfig.WorkersResourcesDistributor workersResourcesDistributor, boolean barrierDisabled
  ) throws InterruptedException, IOException {
    final int inputSize = 100;
    final int frontPort = 4567;
    final int rearPort = 5678;

    final List<String> input = Stream.generate(() -> UUID.randomUUID().toString())
            .limit(inputSize)
            .collect(Collectors.toList());
    final Server front = front(input, frontPort);

    final List<String> result = new ArrayList<>();
    final CountDownLatch latch = new CountDownLatch(inputSize);
    final Server rear = rear(latch, result, rearPort);

    try (final LocalClusterRuntime localClusterRuntime = new LocalClusterRuntime(
            4,
            new SystemConfig.Builder()
                    .workersResourcesDistributor(workersResourcesDistributor)
                    .barrierDisabled(barrierDisabled)
                    .build()
    )) {
      final FlameClient flameClient = new ZkFlameClient(localClusterRuntime.zkString());
      flameClient.push(new Job.Builder(testGraph())
              .addFront(new Job.Front("socket-front", "localhost", frontPort, String.class))
              .addRear(new Job.Rear("socket-rear", "localhost", rearPort))
              .build());

      latch.await(5, TimeUnit.MINUTES);
      Assert.assertEquals(result, input.stream().map(s -> ("prefix_" + s)).collect(Collectors.toList()));
    }

    front.close();
    rear.close();
  }

  private static Server front(List<String> input, int port) {
    final Server producer = new Server(1_000_000, 1000);
    ((Kryo.DefaultInstantiatorStrategy) producer.getKryo().getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    final Connection[] connection = new Connection[1];
    new Thread(() -> {
      synchronized (connection) {
        try {
          connection.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      input.forEach(s -> {
        synchronized (connection) {
          connection[0].sendTCP(s);
          LOG.info("Sending: {}", s);
          LockSupport.parkNanos((long) (50 * 1.0e6));
        }
      });
    }).start();

    producer.addListener(new Listener() {
      @Override
      public void connected(Connection newConnection) {
        synchronized (connection) {
          LOG.info("There is new connection: {}", newConnection.getRemoteAddressTCP());
          //first condition for local testing
          if (connection[0] == null) {
            LOG.info("Accepting connection: {}", newConnection.getRemoteAddressTCP());
            connection[0] = newConnection;
            connection.notify();
          } else {
            LOG.info("Closing connection {}", newConnection.getRemoteAddressTCP());
            newConnection.close();
          }
        }
      }
    });
    producer.start();
    try {
      producer.bind(port);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return producer;
  }

  private static Server rear(CountDownLatch latch, List<String> result, int port) {
    final Server consumer = new Server(2000, 1_000_000);
    ((Kryo.DefaultInstantiatorStrategy) consumer.getKryo()
            .getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
    consumer.getKryo().register(PayloadDataItem.class);
    consumer.getKryo().register(Label.class);
    consumer.getKryo().register(Label[].class);
    consumer.getKryo().register(Labels.class);
    consumer.getKryo().register(Meta.class);
    consumer.getKryo().register(GlobalTime.class);
    consumer.getKryo().register(EdgeId.class);
    consumer.getKryo().register(int[].class);
    consumer.getKryo().register(EdgeId.Limit.class);
    consumer.getKryo().register(Rear.MinTime.class);

    consumer.addListener(new Listener() {
      @Override
      public void connected(Connection connection) {
        LOG.info("Consumer has been connected {}, {}", connection, connection.getRemoteAddressTCP());
      }

      @Override
      public void disconnected(Connection connection) {
        LOG.info("Consumer has been disconnected {}", connection);
      }
    });

    consumer.addListener(new Listener() {
      @Override
      public void received(Connection connection, Object o) {
        if (o instanceof DataItem) {
          LOG.info("Received: {}", o);
          result.add(((DataItem) o).payload(String.class));
          latch.countDown();
        }
      }
    });

    consumer.start();
    try {
      consumer.bind(port);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return consumer;
  }

  private static Graph testGraph() {
    final Source source = new Source();
    final Sink sink = new Sink();

    final FlameMap<String, String> dumbMap =
            new FlameMap.Builder<>((String s) -> Stream.of("prefix_" + s), String.class).build();
    return new Graph.Builder().link(source, dumbMap).link(dumbMap, sink).build(source, sink);
  }
}
