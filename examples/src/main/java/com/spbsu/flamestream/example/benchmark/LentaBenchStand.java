package com.spbsu.flamestream.example.benchmark;

import com.spbsu.flamestream.example.bl.text_classifier.LentaCsvTextDocumentsReader;
import com.spbsu.flamestream.example.bl.text_classifier.TextClassifierGraph;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jooq.lambda.Seq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class LentaBenchStand {
  public static final Class[] CLASSES_TO_REGISTER = {
          Prediction.class, TfIdfObject.class, Topic[].class, Topic.class, HashMap.class, String.class
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
    final BenchStandBuilder benchStandBuilder = new BenchStandBuilder();
    final LentaBenchStand lentaBenchStand = new LentaBenchStand(benchConfig);
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandBuilder.runtime(deployerConfig),
                    new TextClassifierGraph(new SklearnSgdPredictor(
                            "/opt/flamestream/cnt_vectorizer",
                            "/opt/flamestream/classifier_weights"
                    )).get(),
                    new SocketFrontType(lentaBenchStand.benchHost, lentaBenchStand.frontPort, TextDocument.class),
                    new SocketRearType(lentaBenchStand.benchHost, lentaBenchStand.rearPort, CLASSES_TO_REGISTER)
            )
    ) {
      lentaBenchStand.run(graphDeployer);
    }
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(LentaBenchStand.class);

  private final int sleepBetweenDocs;
  private final String lentaDumpPath;
  private final String validatorClass;
  public final String benchHost;
  private final String inputHost;
  public final int frontPort;
  public final int rearPort;
  public final long consumerAwaitTimeoutInMinutes;

  public LentaBenchStand(Config benchConfig) {
    sleepBetweenDocs = benchConfig.getInt("sleep-between-docs-ms");
    lentaDumpPath = benchConfig.getString("lenta-dump-path");
    validatorClass = benchConfig.getString("validator");
    benchHost = benchConfig.getString("bench-host");
    inputHost = benchConfig.getString("input-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
    consumerAwaitTimeoutInMinutes = benchConfig.getLong("consumer-await-timeout-in-minutes");
  }

  public void run(GraphDeployer graphDeployer) throws Exception {
    final BenchStandBuilder benchStandBuilder = new BenchStandBuilder();

    //noinspection unchecked
    BenchValidator<Prediction> validator =
            ((Class<? extends BenchValidator>) Class.forName(validatorClass)).newInstance();
    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(validator.expectedOutputSize());
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    try (
            AutoCloseable ignored = benchStandBuilder.producer(
                    TextDocument.class,
                    documents(validator.inputLimit()).peek(page -> {
                      latencies.put(page.number(), new LatencyMeasurer());
                      LockSupport.parkNanos((long) (nextExp(1.0 / sleepBetweenDocs) * 1.0e6));
                    }),
                    inputHost,
                    frontPort
            )::stop;
            AutoCloseable ignored1 = benchStandBuilder.consumer(
                    Prediction.class,
                    prediction -> {
                      latencies.get(prediction.tfIdf().number()).finish();
                      validator.accept(prediction);
                      awaitConsumer.accept(prediction);
                      if (awaitConsumer.got() % 100 == 0) {
                        LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
                      }
                    },
                    rearPort,
                    CLASSES_TO_REGISTER
            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(consumerAwaitTimeoutInMinutes, TimeUnit.MINUTES);
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
            documents(validator.inputLimit()).map(document -> document.content().length()).collect(Collectors.toList())
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

  private Seq<TextDocument> documents(int maxSize) throws Exception {
    return Seq.seq(LentaCsvTextDocumentsReader.documents(new FileInputStream(lentaDumpPath)))
            .limit(maxSize);
  }

  private final Random payloadDelayRandom = new Random(7);

  private double nextExp(double lambda) {
    return StrictMath.log(1 - payloadDelayRandom.nextDouble()) / -lambda;
  }
}
