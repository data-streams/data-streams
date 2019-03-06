package com.spbsu.flamestream.example.bl.text_classifier;

import akka.actor.ActorSystem;
import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.SingleValueVec;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Optimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SoftmaxRegressionOptimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;
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
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.spbsu.flamestream.example.bl.classifier.PredictorStreamTest.parseDoubles;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertTrue;

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
    });

  }

  @Test
  public void partialFitTest() {
    SparseVec v1 = new SparseVec(5);
    v1.set(2, 1);
    v1.set(3, -1);
    SparseVec v2 = VecTools.copySparse(new SingleValueVec(0.2, 5));
    LOGGER.info("v1 = {}", v1);
    LOGGER.info("v2 = {}", v2);
    VecTools.scale(v1, v2);
    LOGGER.info("scaled v1 = {}", v1);

    final String CNT_VECTORIZER_PATH = "src/main/resources/cnt_vectorizer";
    final String WEIGHTS_PATH = "src/main/resources/classifier_weights";
    final String PATH_TO_TEST_DATA = "src/test/resources/sklearn_prediction";

    final List<String> topics = new ArrayList<>();
    final List<String> texts = new ArrayList<>();
    final List<SparseVec> mx = new ArrayList<>();
    List<Document> documents = new ArrayList<>();
    final SklearnSgdPredictor predictor = new SklearnSgdPredictor(CNT_VECTORIZER_PATH, WEIGHTS_PATH);
    predictor.init();
    try (BufferedReader br = new BufferedReader(new FileReader(new File(PATH_TO_TEST_DATA)))) {
      final double[] data = parseDoubles(br.readLine());
      final int testCount = (int) data[0];
      final int features = (int) data[1];

      for (int i = 0; i < testCount; i++) {
        //final double[] pyPrediction = parseDoubles(br.readLine());

        final String docText = br.readLine().toLowerCase();
        texts.add(docText);

        String topic = br.readLine();
        topics.add(topic);
        final double[] info = parseDoubles(br.readLine());
        final int[] indeces = new int[info.length / 2];
        final double[] values = new double[info.length / 2];
        for (int k = 0; k < info.length; k += 2) {
          final int index = (int) info[k];
          final double value = info[k + 1];

          indeces[k / 2] = index;
          values[k / 2] = value;
        }

        final Map<String, Double> tfIdf = new HashMap<>();
        SparseVec vec = new SparseVec(features, indeces, values);

        SklearnSgdPredictor.text2words(docText).forEach(word -> {
          final int featureIndex = predictor.wordIndex(word);
          tfIdf.put(word, vec.get(featureIndex));
        });
        final Document document = new Document(tfIdf);
        documents.add(document);

        mx.add(vec);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final int len = topics.size();
    final int testsize = 1000;

    List<String> testTopics = topics.stream().skip(len - testsize).collect(Collectors.toList());
    List<String> testTexts = texts.stream().skip(len - testsize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testsize).collect(Collectors.toList());

    SparseMx trainingSet = new SparseMx(mx.stream().limit(len - testsize).toArray(SparseVec[]::new));
    LOGGER.info("Updating weights");
    Optimizer optimizer = new SoftmaxRegressionOptimizer(predictor.getTopics());
    String[] correctTopics = topics.stream().limit(len - testsize).toArray(String[]::new);
    Mx newWeights = optimizer.optimizeWeights(trainingSet, correctTopics, predictor.getWeights());
    predictor.updateWeights(newWeights);

    double truePositives = 0;
    for (int i = 0; i < testsize; i++) {
      String text = testTexts.get(i);
      String ans = testTopics.get(i);
      Document doc = documents.get(i);

      Topic[] prediction = predictor.predict(doc);

      Arrays.sort(prediction);
      if (ans.equals(prediction[0].name())) {
        truePositives++;
      }
      LOGGER.info("Doc: {}", text);
      LOGGER.info("Real answers: {}", ans);
      LOGGER.info("Predict: {}", (Object) prediction);
      LOGGER.info("\n");
    }

    double accuracy = truePositives / testsize;
    LOGGER.info("Accuracy: {}", accuracy);
    assertTrue(accuracy >= 0.62);
  }

  @Test
  public void lentaTest() throws InterruptedException, IOException, TimeoutException {
    final String testFilePath = "lenta/lenta-ru-news.csv";
    final long expectedDocs = documents(testFilePath).count();

    final String cntVectorizerPath = "src/main/resources/cnt_vectorizer";
    final String weightsPath = "src/main/resources/classifier_weights";
    final TopicsPredictor predictor = new SklearnSgdPredictor(cntVectorizerPath, weightsPath);

    final ConcurrentLinkedDeque<Prediction> resultQueue = new ConcurrentLinkedDeque<>();
    final ActorSystem system = ActorSystem.create("lentaTfIdf", ConfigFactory.load("remote"));
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime.Builder().maxElementsInGraph(10)
            .parallelism(2)
            .millisBetweenCommits(1000)
            .build()) {
      try (final FlameRuntime.Flame flame = runtime.run(new TextClassifierGraph(predictor).get())) {
        flame.attachRear("tfidfRear", new AkkaRearType<>(system, Prediction.class))
                .forEach(r -> r.addListener(resultQueue::add));
        final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
                .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(system))
                .collect(toList());

        final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
        for (int i = 1; i < handles.size(); i++) {
          handles.get(i).unregister();
        }

        final AtomicInteger counter = new AtomicInteger(0);
        final Thread thread = new Thread(Unchecked.runnable(() -> {
          final Iterator<TextDocument> toCheckIter = documents(testFilePath).iterator();
          final Map<String, Integer> idfExpected2 = new HashMap<>();
          for (int i = 0; i < expectedDocs; i++) {
            Prediction prediction = resultQueue.poll();
            while (prediction == null) {
              try {
                Thread.sleep(10);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              prediction = resultQueue.poll();
            }

            final int got = counter.incrementAndGet();
            if (got % 1000 == 0) {
              LOGGER.info(String.format("processed %s records", got));
            }

            final TextDocument processedDoc = toCheckIter.next();
            final List<String> pdWords = SklearnSgdPredictor.text2words(processedDoc.content()).collect(toList());
            final Set<String> pdWordsSet = SklearnSgdPredictor.text2words(processedDoc.content())
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
            SklearnSgdPredictor.text2words(processedDoc.content()).forEach(w -> result.merge(w, 1, Integer::sum));
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

            final Topic[] topics = prediction.topics();
            Arrays.sort(topics);
            LOGGER.info("Doc: {}", processedDoc.content());
            LOGGER.info("Predict: {}", (Object) topics);
            LOGGER.info("\n");
          }
        }));

        thread.start();

        documents(testFilePath).forEach(front);

        thread.join();

        Assert.assertEquals(counter.get(), expectedDocs);
      }
    }
    Await.ready(system.terminate(), Duration.Inf());
  }
}
