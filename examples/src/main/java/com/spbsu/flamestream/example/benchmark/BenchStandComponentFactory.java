package com.spbsu.flamestream.example.benchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.RemoteRuntime;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.config.ZookeeperWorkersNode;
import com.spbsu.flamestream.runtime.edge.Rear;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import com.typesafe.config.Config;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.jetbrains.annotations.NotNull;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BenchStandComponentFactory {
  private static final Logger LOG = LoggerFactory.getLogger(BenchStandComponentFactory.class);

  @NotNull
  public <Payload> Server producer(
          Iterable<Payload> payloads,
          int port,
          Stream<String> remotes,
          Class<?> ...classesToRegister
  ) throws IOException {
    List<String> remotesList = remotes.collect(Collectors.toList());
    Map<String, Connection> connections = new HashMap<>();
    CountDownLatch allConnected = new CountDownLatch(remotesList.size());
    new Thread(() -> {
      try {
        allConnected.await();
      } catch (InterruptedException e) {
        throw new RuntimeException();
      }
      int index = 0;
      for (final Payload payload : payloads) {
        index++;
        connections.get(remotesList.get(index % remotesList.size())).sendTCP(payload);
      }
    }).start();
    final Server producer = new Server(200_000_000 / remotesList.size(), 1000 / remotesList.size());
    for (final Class<?> clazz : classesToRegister) {
      producer.getKryo().register(clazz);
    }
    ((Kryo.DefaultInstantiatorStrategy) producer.getKryo()
            .getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    producer.addListener(new Listener() {
      @Override
      public synchronized void received(Connection connection, Object received) {
        if (!(received instanceof String))
          return;
        String id = (String) received;
        final InetSocketAddress connectionAddress = connection.getRemoteAddressTCP();
        LOG.info("There is new connection: {}", connectionAddress);
        if (remotesList.contains(id) && !connections.containsKey(id)) {
          LOG.info("Accepting connection: {}", connectionAddress);
          connections.put(id, connection);
          allConnected.countDown();
        } else {
          LOG.info("Closing connection {}", connectionAddress);
          connection.close();
        }
      }
    });
    producer.start();
    producer.bind(port);
    return producer;
  }

  public Server consumer(Consumer<Object> consumer, int port, Class<?>... classesToRegister) throws IOException {
    final Server server = new Server(2000, 1_000_000);
    for (final Class<?> clazz : classesToRegister) {
      server.getKryo().register(clazz);
    }
    server.getKryo().register(PayloadDataItem.class);
    server.getKryo().register(Meta.class);
    server.getKryo().register(GlobalTime.class);
    server.getKryo().register(EdgeId.class);
    server.getKryo().register(int[].class);
    server.getKryo().register(EdgeId.Min.class);
    server.getKryo().register(EdgeId.Max.class);
    server.getKryo().register(Rear.MinTime.class);
    ((Kryo.DefaultInstantiatorStrategy) server.getKryo()
            .getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    server.addListener(new Listener() {
      @Override
      public void connected(Connection connection) {
        LOG.info("Consumer has been connected {}, {}", connection, connection.getRemoteAddressTCP());
      }

      @Override
      public void disconnected(Connection connection) {
        LOG.info("Consumer has been disconnected {}", connection);
      }
    });

    server.addListener(new Listener() {
      @Override
      public void received(Connection connection, Object object) {
        consumer.accept(object);
      }
    });

    server.start();
    server.bind(port);
    return server;
  }

  public FlameRuntime runtime(Config config) {
    return runtime(config, new SystemConfig.Builder().build());
  }

  public FlameRuntime runtime(Config config, SystemConfig systemConfig) {
    final FlameRuntime runtime;
    if (config.hasPath("local")) {
      runtime = new LocalRuntime.Builder().parallelism(config.getConfig("local").getInt("parallelism")).build();
    } else if (config.hasPath("local-cluster")) {
      runtime = new LocalClusterRuntime(
              config.getConfig("local-cluster").getInt("parallelism"),
              systemConfig
      );
    } else {
      // temporary solution to keep bench stand in working state
      final String zkString = config.getConfig("remote").getString("zk");
      final CuratorFramework curator = CuratorFrameworkFactory.newClient(
              zkString,
              new ExponentialBackoffRetry(1000, 3)
      );
      curator.start();
      try {
        final ZookeeperWorkersNode zookeeperWorkersNode = new ZookeeperWorkersNode(curator, "/workers");
        runtime = new RemoteRuntime(
                curator,
                new KryoSerializer(),
                ClusterConfig.fromWorkers(zookeeperWorkersNode.workers())
        );
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    return runtime;
  }

  public Closeable recordNanoDuration(Writer output) {
    long start = System.nanoTime();
    return () -> output.write(String.valueOf(System.nanoTime() - start));
  }
}
