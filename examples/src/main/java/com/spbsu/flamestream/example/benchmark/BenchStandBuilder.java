package com.spbsu.flamestream.example.benchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.RemoteRuntime;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ZookeeperWorkersNode;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import com.typesafe.config.Config;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class BenchStandBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(BenchStandBuilder.class);

  public <Payload> Server producer(
          Class<Payload> type,
          Stream<Payload> payloadStream,
          String remoteHost,
          int port
  ) throws IOException {
    final Server producer = new Server(1_000_000, 1000);
    producer.getKryo().register(type);
    ((Kryo.DefaultInstantiatorStrategy) producer.getKryo()
            .getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    producer.addListener(new Listener() {
      boolean accepted = false;

      @Override
      public synchronized void connected(Connection newConnection) {
        LOG.info("There is new connection: {}", newConnection.getRemoteAddressTCP());
        try {
          //first condition for local testing
          if (!accepted && newConnection.getRemoteAddressTCP().getAddress().equals(InetAddress.getByName(remoteHost))) {
            accepted = true;
            LOG.info("Accepting connection: {}", newConnection.getRemoteAddressTCP());
            new Thread(() -> {
              int i = 0;
              for (final Payload payload : (Iterable<Payload>) payloadStream::iterator) {
                newConnection.sendTCP(payload);
                LOG.info("Sending: {}", i++);
              }
            }).start();
          } else {
            LOG.info("Closing connection {}", newConnection.getRemoteAddressTCP());
            newConnection.close();
          }
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        }
      }
    });
    producer.start();
    producer.bind(port);
    return producer;
  }

  public <Payload> Server consumer(
          Class<Payload> type,
          Consumer<Payload> consumer,
          int port,
          Class<?> ...classesToRegister
  ) throws IOException {
    final Server server = new Server(2000, 1_000_000);
    for (final Class<?> clazz : classesToRegister) {
      server.getKryo().register(clazz);
    }
    server.getKryo().register(PayloadDataItem.class);
    server.getKryo().register(Meta.class);
    server.getKryo().register(GlobalTime.class);
    server.getKryo().register(EdgeId.class);
    server.getKryo().register(int[].class);
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
        if (object instanceof DataItem) {
          consumer.accept(((DataItem) object).payload(type));
        } else if (type.isInstance(object)) {
          consumer.accept(type.cast(object));
        }
      }
    });

    server.start();
    server.bind(port);
    return server;
  }

  public FlameRuntime runtime(Config config) {
    final FlameRuntime runtime;
    if (config.hasPath("local")) {
      runtime = new LocalRuntime.Builder().parallelism(config.getConfig("local").getInt("parallelism")).build();
    } else if (config.hasPath("local-cluster")) {
      runtime = new LocalClusterRuntime.Builder()
              .parallelism(config.getConfig("local-cluster").getInt("parallelism"))
              .millisBetweenCommits(10000)
              .build();
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
}
