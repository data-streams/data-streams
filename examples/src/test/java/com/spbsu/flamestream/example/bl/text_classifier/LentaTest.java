package com.spbsu.flamestream.example.bl.text_classifier;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.CountVectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.OnlineModel;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.TextUtils;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLProximal;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jooq.lambda.Seq;
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class LentaTest extends FlameAkkaSuite {
  private static final Logger LOGGER = LoggerFactory.getLogger(LentaTest.class);

  private Stream<TextDocument> documents(String path) throws IOException {
    final Reader reader = new InputStreamReader(
            LentaTest.class.getClassLoader().getResourceAsStream(path));
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
              counter.incrementAndGet(),
              r.get(4) // label
      );
    });

  }

  @Test
  public void lentaTest() throws TimeoutException, InterruptedException, IOException {
    final ActorSystem system = ActorSystem.create("lentaTfIdf", ConfigFactory.load("remote"));
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime(
            2,
            new SystemConfig.Builder().maxElementsInGraph(10).millisBetweenCommits(10000).build()
    )) {
      test(runtime, system, 1);
    }
    Await.ready(system.terminate(), Duration.Inf());
  }

  @Test
  public void lentaBlinkTest() throws IOException, InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime.Builder().maxElementsInGraph(100)
            .parallelism(2)
            .withBlink()
            .blinkPeriodSec(7)
            .millisBetweenCommits(2000)
            .build()) {
      test(runtime, runtime.system(), 2);
    }
  }

  private void test(FlameRuntime runtime, ActorSystem system, int realParallelism) throws
                                                                                   IOException,
                                                                                   InterruptedException {
    final String topicsPath = "src/main/resources/topics";
    final String[] topics = TextUtils.readTopics(topicsPath);
    final String cntVectorizerPath = "src/main/resources/cnt_vectorizer";
    final Vectorizer vectorizer = new CountVectorizer(cntVectorizerPath);
    final OnlineModel model = FTRLProximal.builder()
            .alpha(132)
            .beta(0.1)
            .lambda1(0.5)
            .lambda2(0.095)
            .build(topics);

    final ConcurrentLinkedDeque<Prediction> resultQueue = new ConcurrentLinkedDeque<>();
    try (final FlameRuntime.Flame flame = runtime.run(new TextClassifierGraph(vectorizer, model).get())) {
      flame.attachRear("tfidfRear", new AkkaRearType<>(system, Prediction.class))
              .forEach(r -> r.addListener(resultQueue::add));
      final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
              .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(system))
              .collect(toList());

      final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
      for (int i = 1; i < handles.size(); i++) {
        handles.get(i).unregister();
      }

      final String testFilePath = "lenta/lenta-ru-news.csv";
      final long expectedDocs = documents(testFilePath).count();
      final AtomicInteger counter = new AtomicInteger(0);
      final AtomicInteger allCounter = new AtomicInteger(0);
      final Set<String> alreadySeen = new HashSet<>();
      final Thread thread = new Thread(Unchecked.runnable(() -> {
        final Iterator<TextDocument> toCheckIter = documents(testFilePath).iterator();
        final Map<String, Integer> idfExpected2 = new HashMap<>();
        for (int i = 0; i < expectedDocs; i++) {
          Prediction prediction = null;
          while (prediction == null) {
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            prediction = resultQueue.poll();
            if (prediction != null) {
              allCounter.incrementAndGet();
              if (alreadySeen.contains(prediction.tfIdf().document())) {
                prediction = null;
              } else {
                alreadySeen.add(prediction.tfIdf().document());
              }
            }
          }

          final int got = counter.incrementAndGet();
          if (got % 1000 == 0) {
            LOGGER.info(String.format("processed %s records", got));
          }

          final TextDocument processedDoc = toCheckIter.next();
          final List<String> pdWords = TextUtils.text2words(processedDoc.content()).collect(toList());
          final Set<String> pdWordsSet = TextUtils.text2words(processedDoc.content())
                  .collect(Collectors.toSet());
          pdWordsSet.forEach(w -> idfExpected2.merge(w, 1, Integer::sum));
          final TfIdfObject tfIdf = prediction.tfIdf();
          { //runtime info logging
            final Runtime rt = Runtime.getRuntime();
            if (got % 20 == 0) {
              LOGGER.info(
                      "pdWords: %d %d %d/%d %d %d %d %s%n",
                      tfIdf.number(),
                      got,
                      rt.freeMemory(),
                      rt.totalMemory(),
                      pdWords.size(),
                      idfExpected2.size(),
                      i,
                      pdWords
              );
            }
          }

          final Map<String, Integer> result = new HashMap<>();
          TextUtils.text2words(processedDoc.content()).forEach(w -> result.merge(w, 1, Integer::sum));
          Assert.assertEquals(processedDoc.name(), tfIdf.document(), String.format(
                  "unexpected document: '%s' instead of '%s'%n",
                  tfIdf.document(),
                  processedDoc.name()
          ));
          Assert.assertEquals(
                  result.keySet(),
                  tfIdf.words(),
                  String.format("unexpected keys: '%s' instead of '%s'%n", result.keySet(), tfIdf.words())
          );
          for (String key : result.keySet()) {
            Assert.assertEquals(
                    result.get(key).intValue(),
                    tfIdf.tf(key),
                    String.format("incorrect TF value for key %s: %d. Expected: %d%n",
                            key, tfIdf.tf(key), result.get(key)
                    )
            );
            Assert.assertEquals(
                    idfExpected2.get(key).intValue(),
                    tfIdf.idf(key),
                    String.format("incorrect IDF value for key %s: %d. Expected: %d%n",
                            key, tfIdf.idf(key), idfExpected2.get(key)
                    )
            );
          }

          final Topic[] pred = prediction.topics();
          Arrays.sort(pred);
          LOGGER.info("Doc: {}", processedDoc.content());
          LOGGER.info("Predict: {}", (Object) pred);
          LOGGER.info("\n");
        }
      }));

      thread.start();

      final long firstPart = expectedDocs / 2;
      final long secondPart = expectedDocs - firstPart;
      Seq.zipWithIndex(documents(testFilePath)).forEach(objects -> {
        final TextDocument source = objects.v1;
        if (objects.v2 < firstPart) {
          front.accept(source);
        } else {
          front.accept(new TextDocument(source.name(), source.content(), source.partitioning(), source.number(), null));
        }
      });

      thread.join();

      Assert.assertEquals(counter.get(), expectedDocs);
      //noinspection PointlessArithmeticExpression
      Assert.assertEquals(allCounter.get(), secondPart + firstPart * realParallelism);
    }
  }
}
