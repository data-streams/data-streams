package com.spbsu.benchmark.flink.roundrobin.ops;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.jetbrains.annotations.Nullable;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SimpleSource extends RichParallelSourceFunction<Integer> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(SimpleSource.class);
  private static final int INPUT_BUFFER_SIZE = 20_000_000;
  private static final int CONNECTION_AWAIT_TIMEOUT = 5000;

  private final String hostname;
  private final int port;
  private final int watermarkPeriod;
  private int receivedItemsCount = 0;

  @Nullable
  private transient Client client = null;
  private long prevGlobalTs = 0;

  public SimpleSource(String hostname, int port, int watermarkPeriod) {
    this.hostname = hostname;
    this.port = port;
    this.watermarkPeriod = watermarkPeriod;
  }

  @Override
  public void open(Configuration parameters) {
    //tracer = Tracing.TRACING.forEvent("source-receive", 1000, 1);

    client = new Client(1000, INPUT_BUFFER_SIZE);
    client.getKryo().register(Integer.class);
  }

  @Override
  public void run(SourceContext<Integer> ctx) throws Exception {
    final SimpleSource simpleSource = this;

    client.addListener(new Listener() {
      @Override
      public void received(Connection connection, Object object) {
        if (object instanceof Integer) {

          if (object.equals(-1)) {
            simpleSource.cancel();
            client.stop();
          }
          //tracer.log(((WikipediaPage) object).id());

          ctx.collectWithTimestamp((Integer) object, currentTime());
          receivedItemsCount++;
          if (receivedItemsCount % watermarkPeriod == 0) {
            ctx.emitWatermark(new Watermark(currentTime()));
          }
        } else {
          LOG.warn("WTF: {}", object);
        }
      }
    });


    client.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        LOG.info("DISCONNECTED");
        ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
        client.stop();
        simpleSource.cancel();
      }
    });

    LOG.info("Connecting to {}:{}", hostname, port);
    client.start();
    client.connect(CONNECTION_AWAIT_TIMEOUT, hostname, port);
    client.sendTCP(UUID.randomUUID().toString());
    LOG.info("CONNECTED");
    client.run();

    synchronized (hostname) {
      hostname.wait();
    }
  }

  private synchronized long currentTime() {
    long globalTs = System.currentTimeMillis();
    if (globalTs <= prevGlobalTs) {
      globalTs = prevGlobalTs + 1;
    }
    prevGlobalTs = globalTs;
    return globalTs;
  }

  @Override
  public void cancel() {
    synchronized (hostname) {
      hostname.notify();
    }
    final Client theSocket = client;
    if (theSocket != null) {
      theSocket.close();
    }
  }
}