package com.spbu.flamestream.client;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.spbsu.flamestream.core.Job;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class ZkFlameClient implements FlameClient {
  private static final int MAX_BUFFER_SIZE = 20000;
  private static final int BUFFER_SIZE = 1000;

  private final CuratorFramework curator;
  private final Kryo kryo;

  @SuppressWarnings("WeakerAccess")
  public ZkFlameClient(String zkString) {
    curator = CuratorFrameworkFactory.newClient(zkString, new ExponentialBackoffRetry(1000, 3));
    curator.start();

    this.kryo = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
    kryo.getFieldSerializerConfig().setIgnoreSyntheticFields(false);
  }

  @Override
  public void push(Job job) {
    try {
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, job);
      curator.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
              .forPath("/jobs/job", o.toBytes());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    curator.close();
  }
}
