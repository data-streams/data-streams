package com.spbsu.flamestream.example.benchmark;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.example.bl.index.InvertedIndexGraph;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.bl.index.utils.WikipeadiaInput;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jooq.lambda.Seq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class WikiBenchStand {
  public static final Class[] CLASSES_TO_REGISTER = {WordIndexAdd.class, WordIndexRemove.class, long[].class};

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
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();
    final WikiBenchStand wikiBenchStand = new WikiBenchStand(benchConfig);
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandComponentFactory.runtime(deployerConfig),
                    new InvertedIndexGraph().get(),
                    new SocketFrontType(wikiBenchStand.benchHost, wikiBenchStand.frontPort, WikipediaPage.class),
                    new SocketRearType(wikiBenchStand.benchHost, wikiBenchStand.rearPort, CLASSES_TO_REGISTER)
            )
    ) {
      wikiBenchStand.run(graphDeployer, benchConfig.getString("worker-id-prefix") + "0");
    }
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(WikiBenchStand.class);

  private final double sleepBetweenDocs;
  private final String wikiDumpPath;
  private final String validatorClass;
  public final String benchHost;
  private final String inputHost;
  public final int frontPort;
  public final int rearPort;

  public WikiBenchStand(Config benchConfig) {
    sleepBetweenDocs = benchConfig.getDouble("sleep-between-docs-ms");
    wikiDumpPath = benchConfig.getString("wiki-dump-path");
    validatorClass = benchConfig.getString("validator");
    benchHost = benchConfig.getString("bench-host");
    inputHost = benchConfig.getString("input-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
  }

  public void run(GraphDeployer graphDeployer, String inputHostId) throws Exception {
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();

    //noinspection unchecked
    BenchValidator<WordIndexAdd> validator =
            ((Class<? extends BenchValidator>) Class.forName(validatorClass)).newInstance();
    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(validator.expectedOutputSize());
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    try (
            AutoCloseable ignored = benchStandComponentFactory.producer(
                    pages(validator.inputLimit()).peek(page -> {
                      LockSupport.parkNanos((long) (nextExp(1.0 / sleepBetweenDocs) * 1.0e6));
                      latencies.put(page.id(), new LatencyMeasurer());
                    })::iterator,
                    frontPort,
                    Stream.of(inputHostId),
                    WikipediaPage.class
            )::stop;
            AutoCloseable ignored1 = benchStandComponentFactory.consumer(
                    object -> {
                      final WordIndexAdd wordIndexAdd;
                      if (object instanceof DataItem) {
                        wordIndexAdd = ((DataItem) object).payload(WordIndexAdd.class);
                      } else if (object instanceof WordIndexAdd) {
                        wordIndexAdd = (WordIndexAdd) object;
                      } else {
                        return;
                      }
                      latencies.get(IndexItemInLong.pageId(wordIndexAdd.positions()[0])).finish();
                      validator.accept(wordIndexAdd);
                      awaitConsumer.accept(wordIndexAdd);
                      if (awaitConsumer.got() % 10000 == 0) {
                        LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
                      }
                    },
                    rearPort,
                    CLASSES_TO_REGISTER
            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(5, TimeUnit.MINUTES);
      Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
      validator.stop();
    }
    final String latenciesString = latencies.values()
            .stream()
            .map(latencyMeasurer -> Long.toString(latencyMeasurer.statistics().getMax()))
            .collect(Collectors.joining(", "));
    try (final PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get("/tmp/lat.data")))) {
      pw.println(latenciesString);
    }
    LOG.info("Result: {}", latenciesString);
    LOG.info(
            "Page sizes: {}",
            pages(validator.inputLimit()).map(page -> page.text().length()).collect(Collectors.toList())
    );
    final long[] skipped = latencies.values()
            .stream()
            .skip(200)
            .mapToLong(l -> l.statistics().getMax())
            .toArray();
    LOG.info("Median: {}", (long) percentiles().index(50).compute(skipped));
    LOG.info("75%: {}", ((long) percentiles().index(75).compute(skipped)));
    LOG.info("90%: {}", (long) percentiles().index(90).compute(skipped));
    LOG.info("99%: {}", (long) percentiles().index(99).compute(skipped));
  }

  private Seq<WikipediaPage> pages(int maxSize) {
    return Seq.seq(WikipeadiaInput.dumpStreamFromFile(wikiDumpPath)).limit(maxSize).shuffle(new Random(1));
  }

  private final Random payloadDelayRandom = new Random(7);

  private double nextExp(double lambda) {
    return StrictMath.log(1 - payloadDelayRandom.nextDouble()) / -lambda;
  }
}
