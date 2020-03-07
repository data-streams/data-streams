package com.spbsu.flamestream.example.benchmark;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.example.labels.BreadthSearchGraph;
import com.spbsu.flamestream.example.labels.Materializer;
import com.spbsu.flamestream.example.labels.SqliteOutboundEdges;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
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
public class BreadthSearchGraphBenchStand {
  public static final Class<?>[] FRONT_CLASSES_TO_REGISTER = {
          com.spbsu.flamestream.example.labels.BreadthSearchGraph.Request.class,
          com.spbsu.flamestream.example.labels.BreadthSearchGraph.Request.Identifier.class,
          com.spbsu.flamestream.example.labels.BreadthSearchGraph.VertexIdentifier.class
  }, REAR_CLASSES_TO_REGISTER = {
          BreadthSearchGraph.RequestOutput.class,
          BreadthSearchGraph.RequestKey.class,
          BreadthSearchGraph.Request.Identifier.class,
          com.spbsu.flamestream.example.labels.BreadthSearchGraph.VertexIdentifier.class,
          scala.collection.convert.Wrappers.SeqWrapper.class,
          long[].class
  };

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
    final BreadthSearchGraphBenchStand wikiBenchStand = new BreadthSearchGraphBenchStand(benchConfig);
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandComponentFactory.runtime(deployerConfig),
                    Materializer.materialize(BreadthSearchGraph.immutableFlow(SqliteOutboundEdges.INSTANCE)),
                    new SocketFrontType(wikiBenchStand.benchHost, wikiBenchStand.frontPort, FRONT_CLASSES_TO_REGISTER),
                    new SocketRearType(wikiBenchStand.benchHost, wikiBenchStand.rearPort, REAR_CLASSES_TO_REGISTER)
            )
    ) {
      wikiBenchStand.run(graphDeployer, benchConfig.getString("worker-id-prefix") + "0");
    }
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(BreadthSearchGraphBenchStand.class);

  private final int sleepBetweenDocs;
  private final String wikiDumpPath;
  private final String validatorClass;
  public final String benchHost;
  private final String inputHost;
  public final int frontPort;
  public final int rearPort;

  public BreadthSearchGraphBenchStand(Config benchConfig) {
    sleepBetweenDocs = benchConfig.getInt("sleep-between-docs-ms");
    wikiDumpPath = benchConfig.getString("wiki-dump-path");
    validatorClass = benchConfig.getString("validator");
    benchHost = benchConfig.getString("bench-host");
    inputHost = benchConfig.getString("input-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
  }

  public void run(GraphDeployer graphDeployer, String inputHostId) throws Exception {
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();

    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(1);
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    try (
            AutoCloseable ignored = benchStandComponentFactory.producer(
                    Stream.of(new BreadthSearchGraph.Request(
                            new BreadthSearchGraph.Request.Identifier(0),
                            new BreadthSearchGraph.VertexIdentifier(0),
                            1
                    ))::iterator,
                    frontPort,
                    Stream.of(inputHostId),
                    FRONT_CLASSES_TO_REGISTER
            )::stop;
            AutoCloseable ignored1 = benchStandComponentFactory.consumer(
                    object -> {
                      final BreadthSearchGraph.RequestOutput output;
                      if (object instanceof DataItem) {
                        output = ((DataItem) object).payload(BreadthSearchGraph.OUTPUT_CLASS);
                      } else if (object instanceof BreadthSearchGraph.RequestOutput) {
                        output = (BreadthSearchGraph.RequestOutput) object;
                      } else {
                        return;
                      }
                      System.out.println(output);
                      awaitConsumer.accept(output);
                      if (awaitConsumer.got() % 10000 == 0) {
                        LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
                      }
                    },
                    rearPort,
                    REAR_CLASSES_TO_REGISTER
            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(60, TimeUnit.MINUTES);
      Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
    }
    final String latenciesString = latencies.values()
            .stream()
            .map(latencyMeasurer -> Long.toString(latencyMeasurer.statistics().getMax()))
            .collect(Collectors.joining(", "));
    try (final PrintWriter pw = new PrintWriter(Files.newBufferedWriter(Paths.get("/tmp/lat.data")))) {
      pw.println(latenciesString);
    }
    LOG.info("Result: {}", latenciesString);
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

  private final Random payloadDelayRandom = new Random(7);

  private double nextExp(double lambda) {
    return StrictMath.log(1 - payloadDelayRandom.nextDouble()) / -lambda;
  }
}
