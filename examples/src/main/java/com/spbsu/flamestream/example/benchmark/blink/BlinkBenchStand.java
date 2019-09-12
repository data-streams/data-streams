package com.spbsu.flamestream.example.benchmark.blink;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.example.benchmark.BenchValidator;
import com.spbsu.flamestream.example.benchmark.LatencyMeasurer;
import com.spbsu.flamestream.example.bl.index.InvertedIndexGraph;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.bl.index.utils.WikipeadiaInput;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.RemoteRuntime;
import com.spbsu.flamestream.runtime.WorkerApplication;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.serialization.JacksonSerializer;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.jooq.lambda.Seq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class BlinkBenchStand implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BlinkBenchStand.class);

  private final StandConfig standConfig;
  private final BenchValidator<WordIndexAdd> validator;

  private final Random random = new Random(7);
  private final FlameRuntime runtime;

  public BlinkBenchStand(StandConfig standConfig, FlameRuntime runtime) {
    this.standConfig = standConfig;
    this.runtime = runtime;

    try {
      //noinspection unchecked
      validator = ((Class<? extends BenchValidator>) Class.forName(standConfig.validatorClass())).newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public void run() {
    final Map<String, String> props = new HashMap<>();
    props.put("akka.remote.artery.canonical.hostname", standConfig.benchHost());
    props.put("akka.remote.artery.canonical.port", String.valueOf(standConfig.frontPort()));
    final Config config = ConfigFactory.parseMap(props).withFallback(ConfigFactory.load("remote"));
    final ActorSystem system = ActorSystem.create("bench-stand", config);

    final Map<Integer, LatencyMeasurer> measuresByDoc = Collections.synchronizedMap(new LinkedHashMap<>());
    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(validator.expectedOutputSize());

    try (FlameRuntime.Flame flame = runtime.run(new InvertedIndexGraph().get())) {
      flame.attachRear("wikirear", new AkkaRearType<>(system, WordBase.class))
              .forEach(r -> r.addListener(wordBase -> {
                final WordIndexAdd wordIndexAdd;
                if (wordBase instanceof WordIndexAdd) {
                  wordIndexAdd = (WordIndexAdd) wordBase;
                } else {
                  return;
                }
                final int docId = IndexItemInLong.pageId(wordIndexAdd.positions()[0]);

                measuresByDoc.get(docId).finish();
                validator.accept(wordIndexAdd);
                awaitConsumer.accept(wordBase);
                if (awaitConsumer.got() % 10000 == 0) {
                  LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
                }

              }));

      final List<AkkaFront.FrontHandle<Object>> consumers =
              flame.attachFront("wikifront", new AkkaFrontType<>(system, true))
                      .collect(Collectors.toList());

      for (int i = 1; i < consumers.size(); i++) {
        consumers.get(i).unregister();
      }

      final BlockingQueue<WikipediaPage> queue = new LinkedBlockingQueue<>();
      new Thread(() -> {
        final int[] i = {0};
        Seq.seq(WikipeadiaInput.dumpStreamFromFile(standConfig.wikiDumpPath()))
                .limit(validator.inputLimit())
                .shuffle(new Random(1))
                .forEach(page -> {
                  measuresByDoc.put(page.id(), new LatencyMeasurer());
                  LOG.info("Sending: {}", i[0]++);

                  queue.add(page);

                  final long v = (long) (nextExp(1.0 / standConfig.sleepBetweenDocs()) * 1.0e6);
                  LockSupport.parkNanos(v);
                });
      }).start();

      for (int j = 0; j < validator.inputLimit(); ++j) {
        final WikipediaPage page = queue.take();
        consumers.get(0).accept(page);
      }

      final AkkaFront.FrontHandle<Object> sink = consumers.get(0);
      sink.unregister();

      awaitConsumer.await(10, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final long[] latencies = measuresByDoc.values().stream().mapToLong(l -> l.statistics().getMax()).toArray();
    try (final PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get("/tmp/lat.data")))) {
      final String joined = Arrays.stream(latencies).mapToObj(Long::toString).collect(Collectors.joining(", "));
      pw.println(joined);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    LOG.info("Result: {}", Arrays.toString(latencies));
    final List<Integer> collect = Seq.seq(WikipeadiaInput.dumpStreamFromFile(standConfig.wikiDumpPath()))
            .limit(validator.inputLimit())
            .shuffle(new Random(1))
            .map(p -> p.text().length())
            .collect(Collectors.toList());
    LOG.info("Page sizes: {}", collect);
    final long[] skipped = measuresByDoc.values()
            .stream()
            .skip(200)
            .mapToLong(l -> l.statistics().getMax())
            .toArray();
    LOG.info("Median: {}", (long) percentiles().index(50).compute(skipped));
    LOG.info("75%: {}", ((long) percentiles().index(75).compute(skipped)));
    LOG.info("90%: {}", (long) percentiles().index(90).compute(skipped));
    LOG.info("99%: {}", (long) percentiles().index(99).compute(skipped));
  }

  @Override
  public void close() throws Exception {
    runtime.close();
  }

  private double nextExp(double lambda) {
    return StrictMath.log(1 - random.nextDouble()) / -lambda;
  }

  public static class StandConfig {
    private final double sleepBetweenDocs;
    private final String wikiDumpPath;
    private final String validatorClass;
    private final String benchHost;
    private final int frontPort;

    public StandConfig(Config config) {
      sleepBetweenDocs = config.getDouble("sleep-between-docs-ms");
      wikiDumpPath = config.getString("wiki-dump-path");
      validatorClass = config.getString("validator");
      benchHost = config.getString("bench-host");
      frontPort = config.getInt("bench-source-port");
    }

    public String benchHost() {
      return benchHost;
    }

    public int frontPort() {
      return frontPort;
    }

    public double sleepBetweenDocs() {
      return sleepBetweenDocs;
    }

    public String wikiDumpPath() {
      return wikiDumpPath;
    }

    public String validatorClass() {
      return validatorClass;
    }
  }

  public static void main(String[] args) throws Exception {
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
      runtime = new LocalClusterRuntime(
              deployerConfig.getConfig("local-cluster").getInt("parallelism"),
              new SystemConfig.Builder().millisBetweenCommits(10000).build()
      );
    } else {
      final String zkString = deployerConfig.getConfig("remote").getString("zk");
      final CuratorFramework curator = CuratorFrameworkFactory.newClient(
              zkString,
              new ExponentialBackoffRetry(1000, 3)
      );
      curator.start();
      try {
        final ClusterConfig config = new JacksonSerializer().deserialize(
                curator.getData().forPath("/config"),
                ClusterConfig.class
        );
        runtime = new RemoteRuntime(curator, new KryoSerializer(), config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    new BlinkBenchStand(standConfig, runtime).run();
    runtime.close();
    System.exit(0);
  }
}
