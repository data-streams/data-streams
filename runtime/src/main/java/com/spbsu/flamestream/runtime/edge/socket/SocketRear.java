package com.spbsu.flamestream.runtime.edge.socket;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.spbsu.flamestream.core.Batch;
import com.spbsu.flamestream.runtime.edge.Rear;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * User: Artem
 * Date: 29.12.2017
 */
public class SocketRear implements Rear {
  private static final Logger LOG = LoggerFactory.getLogger(SocketRear.class);

  private final EdgeId edgeId;
  private final Client client;

  //private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("rear-in");

  public SocketRear(EdgeContext edgeContext, String host, int port, Class[] classes) {
    edgeId = edgeContext.edgeId();
    client = new Client(1_000_000, 1234);
    Arrays.stream(classes).forEach(clazz -> client.getKryo().register(clazz));
    { //register inners of data item
      client.getKryo().register(PayloadDataItem.class);
      client.getKryo().register(Meta.class);
      client.getKryo().register(GlobalTime.class);
      client.getKryo().register(EdgeId.class);
      client.getKryo().register(int[].class);
    }

    ((Kryo.DefaultInstantiatorStrategy) client.getKryo().getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
    client.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        LOG.info("{}: has been disconnected from {}", edgeId, connection);
      }
    });

    LOG.info("{}: connecting to {}:{}", edgeId, host, port);
    client.start();
    try {
      client.connect(5000, host, port);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.info("{}: connected to {}:{}", edgeId, host, port);
  }

  @Override
  public void accept(Batch batch) {
    if (client.isConnected()) {
      batch.payload(Object.class).forEach(client::sendTCP);
    } else {
      LOG.warn("{}: writing to closed log", edgeId);
    }
  }

  @Override
  public Batch last() {
    return Batch.Default.EMPTY;
  }
}
