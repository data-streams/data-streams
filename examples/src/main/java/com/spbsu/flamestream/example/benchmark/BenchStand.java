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
import com.spbsu.flamestream.example.bl.index.InvertedIndexGraph;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.bl.index.utils.WikipeadiaInput;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.RemoteRuntime;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jooq.lambda.Seq;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class BenchStand implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BenchStand.class);
  private final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());

  private final StandConfig standConfig;
  private final BenchValidator<WordIndexAdd> validator;
  private final GraphDeployer graphDeployer;
  private final Class<?>[] classesToRegister;
  private final AwaitCountConsumer awaitConsumer;

  private final Server producer;
  private final Server consumer;

  public BenchStand(StandConfig standConfig, GraphDeployer graphDeployer, Class<?>... classesToRegister) {
    this.standConfig = standConfig;
    this.graphDeployer = graphDeployer;
    this.classesToRegister = classesToRegister;
    try {
      //noinspection unchecked
      validator = ((Class<? extends BenchValidator>) Class.forName(standConfig.validatorClass())).newInstance();
      awaitConsumer = new AwaitCountConsumer(validator.expectedOutputSize());

      producer = producer();
      consumer = consumer();
    } catch (IOException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public void run() throws InterruptedException {
    graphDeployer.deploy();
    awaitConsumer.await(60, TimeUnit.MINUTES);
  }

  private Server producer() throws IOException {
    final Stream<WikipediaPage> input = Seq.seq(WikipeadiaInput.dumpStreamFromFile(standConfig.wikiDumpPath()))
            .limit(validator.inputLimit())
            .shuffle(new Random(1));
    final Server producer = new Server(1_000_000, 1000);
    producer.getKryo().register(WikipediaPage.class);
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
      final int[] i = {0};
      input.forEach(page -> {
                synchronized (connection) {
                  try {
                    latencies.put(page.id(), new LatencyMeasurer());
                    connection[0].sendTCP(page);
                    LOG.info("Sending: {}", i[0]++);
                    Thread.sleep(standConfig.sleepBetweenDocs());
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }
              }
      );
      connection[0].close();
    }).start();

    producer.addListener(new Listener() {
      @Override
      public void connected(Connection newConnection) {
        synchronized (connection) {
          LOG.info("There is new connection: {}", newConnection.getRemoteAddressTCP());
          try {
            //first condition for local testing
            if (connection[0] == null && newConnection.getRemoteAddressTCP().getAddress()
                    .equals(InetAddress.getByName(standConfig.inputHost()))) {
              LOG.info("Accepting connection: {}", newConnection.getRemoteAddressTCP());
              connection[0] = newConnection;
              connection.notify();
            } else {
              LOG.info("Closing connection {}", newConnection.getRemoteAddressTCP());
              newConnection.close();
            }
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
        }
      }
    });
    producer.start();
    producer.bind(standConfig.frontPort());
    return producer;
  }

  private Server consumer() throws IOException {
    final Server consumer = new Server(2000, 1_000_000);
    Arrays.stream(classesToRegister).forEach(clazz -> consumer.getKryo().register(clazz));
    consumer.getKryo().register(WordIndexAdd.class);
    consumer.getKryo().register(WordIndexRemove.class);
    consumer.getKryo().register(long[].class);
    ((Kryo.DefaultInstantiatorStrategy) consumer.getKryo()
            .getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    consumer.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        LOG.info("Consumer has been disconnected {}", connection);
      }
    });

    consumer.addListener(new Listener() {
      @Override
      public void received(Connection connection, Object o) {
        final WordIndexAdd wordIndexAdd;
        if (o instanceof DataItem) {
          final DataItem dataItem = (DataItem) o;
          wordIndexAdd = dataItem.payload(WordIndexAdd.class);
        } else if (o instanceof WordIndexAdd) {
          wordIndexAdd = (WordIndexAdd) o;
        } else {
          return;
        }
        final int docId = IndexItemInLong.pageId(wordIndexAdd.positions()[0]);
        latencies.get(docId).finish();
        validator.accept(wordIndexAdd);
        awaitConsumer.accept(o);
      }
    });

    consumer.start();
    consumer.bind(standConfig.rearPort());
    return consumer;
  }

  @Override
  public void close() {
    validator.stop();
    graphDeployer.close();
    producer.stop();
    consumer.stop();

    final long[] latencies = this.latencies.values().stream().mapToLong(l -> l.statistics().getMax()).toArray();
    LOG.info("Result: {}", Arrays.toString(latencies));
    final List<Integer> collect = Seq.seq(WikipeadiaInput.dumpStreamFromFile(standConfig.wikiDumpPath()))
            .limit(validator.inputLimit())
            .shuffle(new Random(1))
            .map(p -> p.text().length())
            .collect(Collectors.toList());
    LOG.info("Page sizes: {}", collect);
    final long[] skipped = this.latencies.values()
            .stream()
            .skip(200)
            .mapToLong(l -> l.statistics().getMax())
            .toArray();
    LOG.info("Median: {}", ((long) percentiles().index(50).compute(skipped)));
    LOG.info("75%: {}", ((long) percentiles().index(75).compute(skipped)));
    LOG.info("90%: {}", ((long) percentiles().index(90).compute(skipped)));
    LOG.info("99%: {}", ((long) percentiles().index(99).compute(skipped)));
  }

  public static class StandConfig {
    private final int sleepBetweenDocs;
    private final String wikiDumpPath;
    private final String validatorClass;
    private final String benchHost;
    private final String inputHost;
    private final int frontPort;
    private final int rearPort;

    public StandConfig(Config config) {
      sleepBetweenDocs = config.getInt("sleep-between-docs-ms");
      wikiDumpPath = config.getString("wiki-dump-path");
      validatorClass = config.getString("validator");
      benchHost = config.getString("bench-host");
      inputHost = config.getString("input-host");
      frontPort = config.getInt("bench-source-port");
      rearPort = config.getInt("bench-sink-port");
    }

    public int sleepBetweenDocs() {
      return sleepBetweenDocs;
    }

    public String wikiDumpPath() {
      return wikiDumpPath;
    }

    public String validatorClass() {
      return validatorClass;
    }

    public String benchHost() {
      return benchHost;
    }

    public int frontPort() {
      return frontPort;
    }

    public int rearPort() {
      return rearPort;
    }

    public String inputHost() {
      return this.inputHost;
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    final Config benchConfig;
    final Config deployerConfig;
    if (args.length == 2) {
      benchConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[0]))).getConfig("benchmark");
      deployerConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[1]))).getConfig("deployer");
    } else {
      benchConfig = ConfigFactory.load("bench.conf").getConfig("benchmark");
      deployerConfig = ConfigFactory.load("deployer.conf").getConfig("deployer");
    }
    final StandConfig standConfig = new StandConfig(benchConfig);

    final FlameRuntime runtime;
    if (deployerConfig.hasPath("local")) {
      runtime = new LocalRuntime(deployerConfig.getConfig("local").getInt("parallelism"));
    } else if (deployerConfig.hasPath("local-cluster")) {
      runtime = new LocalClusterRuntime(deployerConfig.getConfig("local-cluster").getInt("parallelism"));
    } else {
      runtime = new RemoteRuntime(deployerConfig.getConfig("remote").getString("zk"));
    }

    final GraphDeployer graphDeployer = new FlameGraphDeployer(
            runtime,
            new InvertedIndexGraph().get(),
            new SocketFrontType(standConfig.benchHost(), standConfig.frontPort(), WikipediaPage.class),
            new SocketRearType(
                    standConfig.benchHost(),
                    standConfig.rearPort(),
                    WordIndexAdd.class,
                    WordIndexRemove.class,
                    long[].class
            )
    );
    try (final BenchStand benchStand = new BenchStand(
            standConfig,
            graphDeployer,
            PayloadDataItem.class,
            Meta.class,
            GlobalTime.class,
            EdgeId.class,
            int[].class
    )) {
      benchStand.run();
    }
    System.exit(0);
  }
}
