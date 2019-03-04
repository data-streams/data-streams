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
import com.spbsu.flamestream.example.bl.text_classifier.TextClassifierGraph;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.RemoteRuntime;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ZookeeperWorkersNode;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.jooq.lambda.Seq;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class LentaBenchStand implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(LentaBenchStand.class);
  private final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());

  private final StandConfig standConfig;
  private final BenchValidator<Prediction> validator;
  private final GraphDeployer graphDeployer;
  private final Class<?>[] classesToRegister;
  private final AwaitCountConsumer awaitConsumer;

  private final Server producer;
  private final Server consumer;
  private final Random random = new Random(7);

  //private final Tracing.Tracer sendTracer = Tracing.TRACING.forEvent("bench-send", 1000, 1);
  //private final Tracing.Tracer receiveTracer = Tracing.TRACING.forEvent("bench-receive");

  public LentaBenchStand(StandConfig standConfig, GraphDeployer graphDeployer, Class<?>... classesToRegister) {
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

  public void run() throws InterruptedException, IOException {
    graphDeployer.deploy();
    awaitConsumer.await(60, TimeUnit.MINUTES);
    producer.close();
    consumer.close();
    Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
  }

  private Stream<TextDocument> documents(String path) throws IOException {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(path),
            StandardCharsets.UTF_8
    ));
    final CSVParser csvFileParser = new CSVParser(reader, CSVFormat.DEFAULT);
    { // skip headers
      Iterator<CSVRecord> iter = csvFileParser.iterator();
      iter.next();
    }

    final Spliterator<CSVRecord> csvSpliterator = Spliterators.spliteratorUnknownSize(
            csvFileParser.iterator(),
            Spliterator.IMMUTABLE
    );
    AtomicInteger counter = new AtomicInteger(0);
    return StreamSupport.stream(csvSpliterator, false).map(r -> {
      Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
      String recordText = r.get(2); // text order
      Matcher m = p.matcher(recordText);
      StringJoiner text = new StringJoiner(" ");
      while (m.find()) {
        text.add(m.group());
      }
      return new TextDocument(
              r.get(0), // url order
              text.toString().toLowerCase(),
              String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
              counter.incrementAndGet()
      );
    });

  }

  private Server producer() throws IOException {
    final Stream<TextDocument> input = Seq.seq(documents(standConfig.wikiDumpPath()))
            .limit(validator.inputLimit())
            .shuffle(new Random(1));
    final Server producer = new Server(1_000_000, 1000);
    producer.getKryo().register(TextDocument.class);
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
                  latencies.put(page.number(), new LatencyMeasurer());
                  //sendTracer.log(page.id());

                  connection[0].sendTCP(page);
                  LOG.info("Sending: {}", i[0]++);
                  final long v = (long) (nextExp(1.0 / standConfig.sleepBetweenDocs()) * 1.0e6);
                  LockSupport.parkNanos(v);
                }
              }
      );
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

  private double nextExp(double lambda) {
    return StrictMath.log(1 - random.nextDouble()) / -lambda;
  }

  private Server consumer() throws IOException {
    final Server consumer = new Server(2000, 1_000_000);
    Arrays.stream(classesToRegister).forEach(clazz -> consumer.getKryo().register(clazz));
    consumer.getKryo().register(Prediction.class);
    consumer.getKryo().register(TfIdfObject.class);
    consumer.getKryo().register(Topic[].class);
    consumer.getKryo().register(Topic.class);
    consumer.getKryo().register(HashMap.class);
    consumer.getKryo().register(String.class);
    ((Kryo.DefaultInstantiatorStrategy) consumer.getKryo()
            .getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

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
        final Prediction prediction;
        if (o instanceof DataItem) {
          final DataItem dataItem = (DataItem) o;
          prediction = dataItem.payload(Prediction.class);
          //receiveTracer.log(((DataItem) o).xor());
        } else if (o instanceof Prediction) {
          prediction = (Prediction) o;
          //receiveTracer.log(wordIndexAdd.hash());
        } else {
          return;
        }
        final int docId = prediction.tfIdf().number();
        //receiveTracer.log(Objects.hash(wordIndexAdd.word(), docId));

        latencies.get(docId).finish();
        validator.accept(prediction);
        awaitConsumer.accept(o);
        if (awaitConsumer.got() % 100 == 0) {
          LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
        }
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
    try (final PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get("/tmp/lat.data")))) {
      final String joined = Arrays.stream(latencies).mapToObj(Long::toString).collect(Collectors.joining(", "));
      pw.println(joined);
      LOG.info("Result: {}", Arrays.toString(latencies));
      final List<Integer> collect = Seq.seq(documents(standConfig.wikiDumpPath()))
              .limit(validator.inputLimit())
              .shuffle(new Random(1))
              .map(p -> p.content().length())
              .collect(Collectors.toList());
      LOG.info("Page sizes: {}", collect);
      final long[] skipped = this.latencies.values()
              .stream()
              .skip(200)
              .mapToLong(l -> l.statistics().getMax())
              .toArray();
      LOG.info("Median: {}", (long) percentiles().index(50).compute(skipped));
      LOG.info("75%: {}", ((long) percentiles().index(75).compute(skipped)));
      LOG.info("90%: {}", (long) percentiles().index(90).compute(skipped));
      LOG.info("99%: {}", (long) percentiles().index(99).compute(skipped));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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
      runtime = new LocalRuntime.Builder().parallelism(deployerConfig.getConfig("local").getInt("parallelism")).build();
    } else if (deployerConfig.hasPath("local-cluster")) {
      runtime = new LocalClusterRuntime.Builder()
              .parallelism(deployerConfig.getConfig("local-cluster").getInt("parallelism"))
              .millisBetweenCommits(10000)
              .build();
    } else {
      // temporary solution to keep bench stand in working state
      final String zkString = deployerConfig.getConfig("remote").getString("zk");
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

    final String cntVectorizerPath = "/opt/flamestream/cnt_vectorizer";
    final String weightsPath = "/opt/flamestream/classifier_weights";
    final TopicsPredictor predictor = new SklearnSgdPredictor(cntVectorizerPath, weightsPath);
    final GraphDeployer graphDeployer = new FlameGraphDeployer(
            runtime,
            new TextClassifierGraph(predictor).get(),
            new SocketFrontType(standConfig.benchHost(), standConfig.frontPort(), TextDocument.class),
            new SocketRearType(
                    standConfig.benchHost(),
                    standConfig.rearPort(),
                    Prediction.class,
                    TfIdfObject.class,
                    Topic[].class,
                    Topic.class,
                    HashMap.class,
                    String.class
            )
    );
    try (final LentaBenchStand benchStand = new LentaBenchStand(
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
