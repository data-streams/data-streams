package com.spbsu.flamestream.example.bl.tfidf;

import akka.actor.ActorSystem;
import com.google.common.collect.Streams;
import com.spbsu.flamestream.example.bl.tfidf.model.Prediction;
import com.spbsu.flamestream.example.bl.tfidf.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.tfidf.model.TextDocument;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier.Topic;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class LentaTest extends FlameAkkaSuite {
  private static final Logger LOGGER = LoggerFactory.getLogger(LentaTest.class);

  private Stream<TextDocument> documents() throws IOException {
    ClassLoader classLoader = LentaTest.class.getClassLoader();
    Reader reader = new InputStreamReader(
            classLoader.getResourceAsStream("lenta/lenta-ru-news.csv"));
    CSVParser csvFileParser = new CSVParser(reader, CSVFormat.DEFAULT);

    Iterator<CSVRecord> iter = csvFileParser.iterator();
    iter.next(); // skip headers

    Spliterator<CSVRecord> csvSpliterator = Spliterators.spliteratorUnknownSize(csvFileParser.iterator(), Spliterator.IMMUTABLE);
    Stream<CSVRecord> records = StreamSupport.stream(csvSpliterator, false);

    final int DATASET_SIZE = 10000;
    AtomicInteger counter = new AtomicInteger(0);
    return records.map(r -> {
      Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
      String recordText = r.get(2); // text order
      Matcher m = p.matcher(recordText);
      StringBuilder text = new StringBuilder();
      while (m.find()) {
        text.append(" ");
        text.append(m.group());
      }
      return new TextDocument(
              r.get(0), // url order
              text.substring(1).toLowerCase(),
              String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
              counter.incrementAndGet()
      );
    }).limit(DATASET_SIZE);
  }

  private Stream<TextDocument> documentsWithTopics() throws IOException {
    ClassLoader classLoader = LentaTest.class.getClassLoader();
    Reader reader = new InputStreamReader(
            classLoader.getResourceAsStream("lenta/lenta-ru-news.csv"));
    CSVParser csvFileParser = new CSVParser(reader, CSVFormat.DEFAULT);

    Iterator<CSVRecord> iter = csvFileParser.iterator();
    iter.next(); // skip headers

    Spliterator<CSVRecord> csvSpliterator = Spliterators.spliteratorUnknownSize(csvFileParser.iterator(), Spliterator.IMMUTABLE);
    Stream<CSVRecord> records = StreamSupport.stream(csvSpliterator, false);

    final int DATASET_SIZE = 10;
    AtomicInteger counter = new AtomicInteger(0);

    Stream<TextDocument>  docs = records.map(r -> {
      Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
      String recordText = r.get(2); // text order
      Matcher m = p.matcher(recordText);
      StringBuilder text = new StringBuilder();
      while (m.find()) {
        text.append(" ");
        text.append(m.group());
      }
      return new TextDocument(
              r.get(0), // url order
              text.substring(1).toLowerCase(),
              String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
              counter.incrementAndGet()
      );
    }).limit(DATASET_SIZE);

    return Streams.concat(docs,
            Stream.of(
                    new TextDocument("11111111", "qqq aaa ccc",
                            String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
                            counter.incrementAndGet(), new Topic[] {}, 1)),
            Stream.of(
                    new TextDocument("22222222", "qqq adfeerwfewaa ccc",
                            String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
                            counter.incrementAndGet(), new Topic[] {}, 1)),
            Stream.of(
                    new TextDocument("333333", "qqq adfeerwfewaa ccc",
                            String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
                            counter.incrementAndGet(), new Topic[] {}, 1)),
            Stream.of(
                    new TextDocument("4444444", "qqq adfeerwfewaa ccc",
                            String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
                            counter.incrementAndGet(), new Topic[] {}, 1))
            );
  }


  @Test
  public void lentaTest() throws InterruptedException, IOException, TimeoutException {
    Stream<TextDocument> toCheck = documents();
    int nExpected = documents().toArray().length;
    final ConcurrentLinkedDeque resultQueue = new ConcurrentLinkedDeque<Object>();

    ActorSystem system = ActorSystem.create("lentaTfIdf", ConfigFactory.load("remote"));
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime.Builder().maxElementsInGraph(10)
            .parallelism(2)
            .millisBetweenCommits(1000)
            .build()) {

      final FlameRuntime.Flame flame = runtime.run(new TfIdfGraph().get());

      flame.attachRear("tfidfRear", new AkkaRearType<>(system, Object.class))
              .forEach(r -> r.addListener(object -> resultQueue.add(object)));
      final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
              .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(system))
              .collect(toList());

      final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
      for (int i = 1; i < handles.size(); i++) {
        handles.get(i).unregister();
      }

      final AtomicInteger counter = new AtomicInteger(0);
      final List<String> errors = new ArrayList<String>();
      Thread t = new Thread(() -> {
        Iterator<TextDocument> toCheckIter = toCheck.iterator();
        //IDFData idfExpected = new IDFData();
        Map<String, Integer> idfExpected2 = new HashMap<>();
        for (int i = 0; i < nExpected; i++) {
          Object o = resultQueue.poll();


          while (o == null) {
            try {
                 Thread.sleep(10);
            } catch (InterruptedException e) {}
            o = resultQueue.poll();
          }

          final int got = counter.incrementAndGet();
          if (got % 1000 == 0) {
            LOGGER.info(String.format("processed %s records", got));
          }

          if (o instanceof Prediction) {
            TextDocument processedDoc = toCheckIter.next();
            List<String> pdWords = TextUtils.words(processedDoc.content());
            Set<String> pdWordsSet = TextUtils.wordsSet(processedDoc.content());

            pdWordsSet.forEach(w -> idfExpected2.merge(w, 1, Integer::sum));

            Runtime rt = Runtime.getRuntime();
            Prediction prediction = (Prediction) o;
            TfIdfObject tfIdf = prediction.tfIdf();

            if (prediction.topics() == null) {
              if (got % 20 == 0) {
                System.out.format(
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
              Map<String, Integer> pdTF = TextUtils.tfData(processedDoc.content());

              if (!Objects.equals(processedDoc.name(), tfIdf.document())) {
                errors.add(String.format(
                        "unexpected document: '%s' instead of '%s'%n",
                        tfIdf.document(),
                        processedDoc.name()
                ));
              }
              if (!Objects.equals(pdTF.keySet(), tfIdf.tfKeys())) {
                errors.add(String.format("unexpected keys: '%s' instead of '%s'%n", pdTF.keySet(), tfIdf.tfKeys()));
              }
              for (String key : pdTF.keySet()) {
                if (pdTF.get(key) != tfIdf.tfCount(key)) {
                  errors.add(String.format("incorrect TF value for key %s: %d. Expected: %d%n",
                          key, tfIdf.tfCount(key), pdTF.get(key)));
                }
                if (idfExpected2.get(key) != tfIdf.idfCount(key)) {
                  errors.add(String.format("incorrect IDF value for key %s: %d. Expected: %d%n",
                          key, tfIdf.idfCount(key), idfExpected2.get(key)));
                }
              }
              if (errors.size() > 0) break;
            } else {
              // classifier results

              if (got % 1000 == 0) {
                final Topic[] topics = prediction.topics();
                Arrays.sort(topics);
                LOGGER.info("Doc: {}", processedDoc.content());
                LOGGER.info("Predict: {}", (Object) prediction);
                LOGGER.info("\n");
              }
            }
          } else {
            errors.add("unexpected: " + o);
          }

        }
      });

      t.start();

      documents().forEach(front);

      t.join();

      Assert.assertEquals(counter.get(), nExpected);
      Assert.assertEquals(errors.size(), 0);
    }
    Await.ready(system.terminate(), Duration.Inf());
  }

  @Test
  public void lenta2Test() throws InterruptedException, IOException, TimeoutException {
    Stream<TextDocument> toCheck = documentsWithTopics();
    int nExpected = documentsWithTopics().filter(e -> e.topics() == null).toArray().length;
    System.out.println("nExpected: " + nExpected);
    final ConcurrentLinkedDeque resultQueue = new ConcurrentLinkedDeque<Object>();

    ActorSystem system = ActorSystem.create("lentaTfIdf", ConfigFactory.load("remote"));
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime.Builder().maxElementsInGraph(10)
            .parallelism(2)
            .millisBetweenCommits(1000)
            .build()) {

      final FlameRuntime.Flame flame = runtime.run(new TfIdfGraph().get());

      flame.attachRear("tfidfRear", new AkkaRearType<>(system, Object.class))
              .forEach(r -> r.addListener(object -> resultQueue.add(object)));
      final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
              .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(system))
              .collect(toList());

      final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
      for (int i = 1; i < handles.size(); i++) {
        handles.get(i).unregister();
      }

      final AtomicInteger counter = new AtomicInteger(0);
      final List<String> errors = new ArrayList<String>();
      Thread t = new Thread(() -> {
        Iterator<TextDocument> toCheckIter = toCheck.iterator();
        //IDFData idfExpected = new IDFData();
        Map<String, Integer> idfExpected2 = new HashMap<>();
        for (int i = 0; i < nExpected; i++) {
          Object o = resultQueue.poll();


          while (o == null) {
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {}
            o = resultQueue.poll();
          }

          final int got = counter.incrementAndGet();
          if (got % 1000 == 0) {
            LOGGER.info(String.format("processed %s records", got));
          }

          if (o instanceof Prediction) {
            TextDocument processedDoc = toCheckIter.next();
            List<String> pdWords = TextUtils.words(processedDoc.content());
            Set<String> pdWordsSet = TextUtils.wordsSet(processedDoc.content());

            pdWordsSet.forEach(w -> idfExpected2.merge(w, 1, Integer::sum));

            Runtime rt = Runtime.getRuntime();
            Prediction prediction = (Prediction) o;
            TfIdfObject tfIdf = prediction.tfIdf();

            if (prediction.topics() == null) {
              if (got % 20 == 0) {
                System.out.format(
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
              Map<String, Integer> pdTF = TextUtils.tfData(processedDoc.content());

              if (!Objects.equals(processedDoc.name(), tfIdf.document())) {
                errors.add(String.format(
                        "unexpected document: '%s' instead of '%s'%n",
                        tfIdf.document(),
                        processedDoc.name()
                ));
              }
              if (!Objects.equals(pdTF.keySet(), tfIdf.tfKeys())) {
                errors.add(String.format("unexpected keys: '%s' instead of '%s'%n", pdTF.keySet(), tfIdf.tfKeys()));
              }
              for (String key : pdTF.keySet()) {
                if (pdTF.get(key) != tfIdf.tfCount(key)) {
                  errors.add(String.format("incorrect TF value for key %s: %d. Expected: %d%n",
                          key, tfIdf.tfCount(key), pdTF.get(key)));
                }
                if (idfExpected2.get(key) != tfIdf.idfCount(key)) {
                  errors.add(String.format("incorrect IDF value for key %s: %d. Expected: %d%n",
                          key, tfIdf.idfCount(key), idfExpected2.get(key)));
                }
              }
              if (errors.size() > 0) break;
            } else {
              // classifier results

              if (got % 1000 == 0) {
                final Topic[] topics = prediction.topics();
                Arrays.sort(topics);
                LOGGER.info("Doc: {}", processedDoc.content());
                LOGGER.info("Predict: {}", prediction);
                LOGGER.info("\n");
              }
            }
          } else {
            errors.add("unexpected: " + o);
          }

        }
      });

      t.start();

      documentsWithTopics().forEach(front);

      t.join();

      Assert.assertEquals(counter.get(), nExpected);
      Assert.assertEquals(errors.size(), 0);
    }
    Await.ready(system.terminate(), Duration.Inf());
  }
}
