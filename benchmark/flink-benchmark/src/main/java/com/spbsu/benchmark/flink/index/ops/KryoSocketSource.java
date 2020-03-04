package com.spbsu.benchmark.flink.index.ops;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.jetbrains.annotations.Nullable;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoSocketSource extends RichParallelSourceFunction<WikipediaPage> implements CheckpointedFunction {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(KryoSocketSource.class);
  private static final int INPUT_BUFFER_SIZE = 20_000_000;
  private static final int CONNECTION_AWAIT_TIMEOUT = 5000;

  private final String hostname;
  private final int port;

  //private transient Tracing.Tracer tracer;

  @Nullable
  private transient Client client = null;
  private long prevGlobalTs = 0;

  public KryoSocketSource(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public void open(Configuration parameters) {
    //tracer = Tracing.TRACING.forEvent("source-receive", 1000, 1);

    client = new Client(1000, INPUT_BUFFER_SIZE);
    client.getKryo().register(WikipediaPage.class);
    ((Kryo.DefaultInstantiatorStrategy) client.getKryo()
            .getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public void run(SourceContext<WikipediaPage> ctx) throws Exception {
    client.addListener(new Listener() {
      @Override
      public void received(Connection connection, Object object) {
        if (object instanceof WikipediaPage) {
          //tracer.log(((WikipediaPage) object).id());

          ctx.collectWithTimestamp((WikipediaPage) object, currentTime());
          ctx.emitWatermark(new Watermark(currentTime()));
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
      }
    });

    LOG.info("Connecting to {}:{}", hostname, port);
    client.start();
    client.connect(CONNECTION_AWAIT_TIMEOUT, hostname, port);
    client.sendTCP("");
    LOG.info("CONNECTED");
    client.run();

    synchronized (hostname) {
      hostname.wait();
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    //do nothing
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    //do nothing
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