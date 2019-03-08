package com.spbsu.flamestream.example.benchmark;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.example.bl.text_classifier.TextClassifierGraph;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.RemoteRuntime;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ZookeeperWorkersNode;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.jooq.lambda.Seq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.math.Quantiles.percentiles;
import static java.util.stream.Collectors.toList;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class LentaBenchStand {
  private static final Logger LOG = LoggerFactory.getLogger(LentaBenchStand.class);

  private static Stream<TextDocument> documents(String path) throws IOException {
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

  public static class StandConfig {
    private final String wikiDumpPath;
    private final String validatorClass;

    StandConfig(Config config) {
      wikiDumpPath = config.getString("wiki-dump-path");
      validatorClass = config.getString("validator");
    }

    String wikiDumpPath() {
      return wikiDumpPath;
    }

    String validatorClass() {
      return validatorClass;
    }
  }

  public static void main(String[] args) throws
                                         IOException,
                                         InterruptedException,
                                         ClassNotFoundException,
                                         IllegalAccessException,
                                         InstantiationException {
    final String cntVectorizerPath = "/opt/flamestream/cnt_vectorizer";
    final String weightsPath = "/opt/flamestream/classifier_weights";
    final TopicsPredictor predictor = new SklearnSgdPredictor(cntVectorizerPath, weightsPath);

    final Config benchConfig;
    final Config deployerConfig;
    //noinspection Duplicates
    if (args.length == 2) {
      benchConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[0]))).getConfig("benchmark");
      deployerConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[1]))).getConfig("deployer");
    } else {
      benchConfig = ConfigFactory.load("bench.conf").getConfig("benchmark");
      deployerConfig = ConfigFactory.load("deployer.conf").getConfig("deployer");
    }
    final StandConfig standConfig = new StandConfig(benchConfig);
    //noinspection unchecked
    final BenchValidator<Prediction> validator = ((Class<? extends BenchValidator>) Class.forName(standConfig.validatorClass()))
            .newInstance();
    final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(validator.expectedOutputSize());
    final Map<Integer, LatencyMeasurer> latenciesMap = Collections.synchronizedMap(new LinkedHashMap<>());

    final Map<String, String> props = new HashMap<>();
    final DumbInetSocketAddress socketAddress = new DumbInetSocketAddress(System.getenv("LOCAL_ADDRESS"));
    props.put("akka.remote.artery.canonical.hostname", socketAddress.host());
    props.put("akka.remote.artery.canonical.port", String.valueOf(socketAddress.port()));
    props.put("akka.remote.artery.bind.hostname", "0.0.0.0");
    props.put("akka.remote.artery.bind.port", String.valueOf(socketAddress.port()));
    try {
      final File shm = new File(("/dev/shm"));
      if (shm.exists() && shm.isDirectory()) {
        final String aeronDir = "/dev/shm/aeron-bench";
        FileUtils.deleteDirectory(new File(aeronDir));
        props.put("akka.remote.artery.advanced.aeron-dir", aeronDir);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    final ActorSystem system = ActorSystem.create(
            "lentaTfIdf",
            ConfigFactory.parseMap(props).withFallback(ConfigFactory.load("remote"))
    );

    final FlameRuntime runtime;
    if (deployerConfig.hasPath("local")) {
      runtime = new LocalRuntime.Builder().parallelism(deployerConfig.getConfig("local").getInt("parallelism")).build();
    } else if (deployerConfig.hasPath("local-cluster")) {
      runtime = new LocalClusterRuntime.Builder()
              .parallelism(deployerConfig.getConfig("local-cluster").getInt("parallelism"))
              .millisBetweenCommits(1000)
              .maxElementsInGraph(100)
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

    try (final FlameRuntime.Flame flame = runtime.run(new TextClassifierGraph(predictor).get())) {
      flame.attachRear("tfidfRear", new AkkaRearType<>(system, Prediction.class))
              .forEach(r -> r.addListener(prediction -> {
                final int docId = prediction.tfIdf().number();
                latenciesMap.get(docId).finish();
                validator.accept(prediction);
                awaitConsumer.accept(prediction);
                LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
              }));
      final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
              .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(system))
              .collect(toList());

      final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
      for (int i = 1; i < handles.size(); i++) {
        handles.get(i).unregister();
      }
      documents(standConfig.wikiDumpPath()).forEach(textDocument -> {
        front.accept(textDocument);
        latenciesMap.put(textDocument.number(), new LatencyMeasurer());
      });
      awaitConsumer.await(60, TimeUnit.MINUTES);
    }

    final long[] latencies = latenciesMap.values().stream().mapToLong(l -> l.statistics().getMax()).toArray();
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
      final long[] skipped = latenciesMap.values()
              .stream()
              .skip(300)
              .mapToLong(l -> l.statistics().getMax())
              .toArray();
      LOG.info("Median: {}", (long) percentiles().index(50).compute(skipped));
      LOG.info("75%: {}", ((long) percentiles().index(75).compute(skipped)));
      LOG.info("90%: {}", (long) percentiles().index(90).compute(skipped));
      LOG.info("99%: {}", (long) percentiles().index(99).compute(skipped));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    System.exit(0);
  }
}
