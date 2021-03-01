package com.spbsu.benchmark.flink.roundrobin.ops;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.jetbrains.annotations.Nullable;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class SimpleSink extends RichSinkFunction<Integer> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SimpleSink.class);
  private static final int OUTPUT_BUFFER_SIZE = 1_000_000;
  private static final int CONNECTION_AWAIT_TIMEOUT = 5000;

  private final String hostName;
  private final int port;

  @Nullable
  private transient Client client = null;

  public SimpleSink(String hostName, int port) {
    this.hostName = hostName;
    this.port = port;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    client = new Client(OUTPUT_BUFFER_SIZE, 1234);
    client.getKryo().register(Integer.class);
    ((Kryo.DefaultInstantiatorStrategy) client.getKryo()
            .getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    client.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        LOG.warn("Sink has been disconnected {}", connection);
      }
    });

    LOG.info("Connecting to {}:{}", hostName, port);
    client.start();
    client.connect(CONNECTION_AWAIT_TIMEOUT, hostName, port);
    LOG.info("Connected to {}:{}", hostName, port);
  }

  @Override
  public void invoke(Integer value, Context context) {
    if (client != null && client.isConnected()) {
      client.sendTCP(value);
    } else {
      throw new RuntimeException("Writing to the closed log");
    }
  }

  @Override
  public void close() {
    try {
      Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (client != null) {
      LOG.info("Closing sink connection");
      client.close();
      client.stop();
    }
  }
}

