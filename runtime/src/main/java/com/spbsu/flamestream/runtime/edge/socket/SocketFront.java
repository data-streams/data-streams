package com.spbsu.flamestream.runtime.edge.socket;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.spbsu.flamestream.runtime.edge.Front;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class SocketFront extends Front.Stub {
  private final static Logger LOG = LoggerFactory.getLogger(SocketFront.class);

  private final Client client;
  private final String host;
  private final int port;

  private volatile Consumer<Object> consumer = null;

  //private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("front-receive-send", 1000, 1);

  public SocketFront(EdgeContext edgeContext, String host, int port, Class<?>[] classes) {
    super(edgeContext.edgeId());
    this.host = host;
    this.port = port;
    client = new Client(1000, 20_000_000);
    Arrays.stream(classes).forEach(clazz -> client.getKryo().register(clazz));
    ((Kryo.DefaultInstantiatorStrategy) client.getKryo().getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    client.addListener(new Listener() {
      public void received(Connection connection, Object object) {
        if (Arrays.stream(classes).anyMatch(clazz -> clazz.isAssignableFrom(object.getClass()))) {
          //tracer.log(object.hashCode());
          final GlobalTime time = currentTime();
          consumer.accept(new PayloadDataItem(new Meta(time), object));
          consumer.accept(new Heartbeat(new GlobalTime(time.time() + 1, time.frontId())));
        }
      }
    });
    client.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        LOG.info("{} has been disconnected from {}", edgeId, connection);
        client.stop();
        consumer.accept(new UnregisterFront(edgeId));
      }
    });
  }

  @Override
  public void onStart(Consumer<Object> consumer, GlobalTime from) {
    final boolean init = this.consumer == null;
    this.consumer = consumer;
    if (init) {
      LOG.info("{}: connecting to {}:{}", edgeId, host, port);
      client.start();
      try {
        client.connect(20000, host, port);
        client.sendTCP(edgeId.nodeId());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void onRequestNext() {
    //socket front does not support backpressure
  }

  @Override
  public void onCheckpoint(GlobalTime to) {
    //socket front does not support checkpoints
  }
}
