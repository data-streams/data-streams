package com.spbsu.flamestream.example.benchmark;

import com.esotericsoftware.kryonet.Connection;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.example.bl.text_classifier.TextClassifierGraph;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.CountVectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.TextUtils;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLProximal;
import com.spbsu.flamestream.example.labels.BinarySocialGraph;
import com.spbsu.flamestream.example.labels.BreadthSearchGraph;
import com.spbsu.flamestream.example.labels.Materializer;
import com.spbsu.flamestream.example.labels.SqliteOutboundEdges;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class LentaBenchStand {
  public static final Class<?>[] FRONT_CLASSES_TO_REGISTER = {
          TextDocument.class
  }, REAR_CLASSES_TO_REGISTER = {
          Prediction.class,
          TfIdfObject.class,
          HashMap.class,
          Topic.class,
          Topic[].class,
          };
  private final int streamLength;
  private final int parallelism;

  private static Stream<TextDocument> documents(String path) throws IOException {
    final BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(path),
            StandardCharsets.UTF_8
    ));
    final CSVParser csvFileParser = new CSVParser(reader, CSVFormat.DEFAULT);
    { // skip headers
      csvFileParser.iterator().next();
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
              String.valueOf(ThreadLocalRandom.current().nextInt()),
              counter.incrementAndGet(),
              r.get(4) // label
      );
    });
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
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();
    final LentaBenchStand wikiBenchStand = new LentaBenchStand(benchConfig);
    final String workerIdPrefix = benchConfig.getString("worker-id-prefix");
    final SystemConfig.WorkersResourcesDistributor.Enumerated workersResourcesDistributor =
            new SystemConfig.WorkersResourcesDistributor.Enumerated(workerIdPrefix, 1);
    final String cntVectorizerPath = "/opt/flamestream/cnt_vectorizer";
    final Vectorizer vectorizer = new CountVectorizer(cntVectorizerPath);
    try (
            GraphDeployer graphDeployer = new FlameGraphDeployer(
                    benchStandComponentFactory.runtime(
                            deployerConfig,
                            new SystemConfig.Builder().millisBetweenCommits(1_000_000_000)
                                    .workersResourcesDistributor(workersResourcesDistributor)
                                    .build()
                    ),
                    new TextClassifierGraph(
                            vectorizer,
                            FTRLProximal.builder()
                                    .alpha(132)
                                    .beta(0.1)
                                    .lambda1(0.5)
                                    .lambda2(0.095)
                                    .build(TextUtils.readTopics("/opt/flamestream/topics"))
                    ).get(),
                    new SocketFrontType(wikiBenchStand.benchHost, wikiBenchStand.frontPort, FRONT_CLASSES_TO_REGISTER),
                    new SocketRearType(wikiBenchStand.benchHost, wikiBenchStand.rearPort, REAR_CLASSES_TO_REGISTER)
            )
    ) {
      final String frontsNumber = System.getenv("FRONTS_NUMBER");
      wikiBenchStand.run(
              graphDeployer,
              IntStream.range(1, frontsNumber == null ? wikiBenchStand.parallelism : Integer.parseInt(frontsNumber))
                      .mapToObj(i -> workerIdPrefix + i).collect(Collectors.toList())
      );
    }
    System.exit(0);
  }

  private static final Logger LOG = LoggerFactory.getLogger(LentaBenchStand.class);

  private final int sleepBetweenDocs;
  public final String benchHost;
  public final int frontPort;
  public final int rearPort;
  public final String documentsPath;

  public LentaBenchStand(Config benchConfig) {
    streamLength = benchConfig.getInt("stream-length");
    sleepBetweenDocs = benchConfig.getInt("sleep-between-docs-ms");
    benchHost = benchConfig.getString("bench-host");
    frontPort = benchConfig.getInt("bench-source-port");
    rearPort = benchConfig.getInt("bench-sink-port");
    parallelism = benchConfig.getInt("parallelism");
    documentsPath = benchConfig.getString("lenta-ru-news-path");
  }

  public void run(GraphDeployer graphDeployer, List<String> inputHosts) throws Exception {
    final BenchStandComponentFactory benchStandComponentFactory = new BenchStandComponentFactory();
    final AwaitCountConsumer awaitConsumer =
            new AwaitCountConsumer((int) documents(documentsPath).count() * (parallelism - 1));
    final Map<Integer, LatencyMeasurer> latencies = Collections.synchronizedMap(new LinkedHashMap<>());
    final CompletableFuture<Map<String, Connection>> producerConnections = new CompletableFuture<>();
    final Thread connectionsAwaiter = new Thread(() -> {
      try {
        final Map<String, Connection> connections = producerConnections.get();
        final ScheduledExecutorService progressLogger = Executors.newSingleThreadScheduledExecutor();
        progressLogger.scheduleAtFixedRate(
                () -> System.out.println("Progress: " + awaitConsumer.got() + "/" + awaitConsumer.expected()),
                0,
                1,
                TimeUnit.SECONDS
        );
        final Iterator<TextDocument> iterator = documents(documentsPath).iterator();
        final ScheduledExecutorService producer = Executors.newSingleThreadScheduledExecutor();
        producer.scheduleAtFixedRate(new Runnable() {
          Iterator<Connection> connectionIterator = connections.values().iterator();

          @Override
          public void run() {
            if (!iterator.hasNext()) {
              producer.shutdownNow();
              progressLogger.shutdown();
              return;
            }
            if (!connectionIterator.hasNext()) {
              connectionIterator = connections.values().iterator();
            }
            final Connection connection = connectionIterator.next();
            final TextDocument textDocument = iterator.next();
            final int requestId = textDocument.number();
            latencies.put(requestId, new LatencyMeasurer());
            connection.sendTCP(textDocument);
          }
        }, 0, sleepBetweenDocs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignored) {
      } catch (ExecutionException | IOException e) {
        throw new RuntimeException(e);
      }
    });
    connectionsAwaiter.start();
    try (
            AutoCloseable ignored = benchStandComponentFactory.producerConnections(
                    producerConnections::complete,
                    frontPort,
                    inputHosts,
                    FRONT_CLASSES_TO_REGISTER
            )::stop;
            AutoCloseable ignored1 = benchStandComponentFactory.consumer(
                    object -> {
                      final Prediction prediction;
                      if (object instanceof DataItem) {
                        prediction = ((DataItem) object).payload(Prediction.class);
                      } else if (object instanceof Prediction) {
                        prediction = (Prediction) object;
                      } else {
                        return;
                      }
                      latencies.get(prediction.tfIdf().number()).finish();
                      awaitConsumer.accept(prediction);
                      if (awaitConsumer.got() % 10000 == 0) {
                        LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
                      }
                    },
                    rearPort,
                    REAR_CLASSES_TO_REGISTER
            )::stop
    ) {
      graphDeployer.deploy();
      awaitConsumer.await(1, TimeUnit.MINUTES);
      connectionsAwaiter.interrupt();
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
            .mapToLong(l -> l.statistics().getMax())
            .toArray();
    LOG.info("Median: {}", (long) percentiles().index(50).compute(skipped));
    LOG.info("75%: {}", ((long) percentiles().index(75).compute(skipped)));
    LOG.info("90%: {}", (long) percentiles().index(90).compute(skipped));
    LOG.info("99%: {}", (long) percentiles().index(99).compute(skipped));
  }
}
