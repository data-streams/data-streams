package com.spbsu.flamestream.example.bl.classifier;

import akka.actor.ActorSystem;
import com.expleague.commons.math.MathTools;
import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Optimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SoftmaxRegressionOptimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static org.testng.Assert.assertTrue;

public class PredictorTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PredictorTest.class.getName());
  private static final String CNT_VECTORIZER_PATH = "src/main/resources/cnt_vectorizer";
  private static final String WEIGHTS_PATH = "src/main/resources/classifier_weights";
  private static final String PATH_TO_TEST_DATA = "src/test/resources/sklearn_prediction";
  private static final String PATH_TO_DATA = "src/test/resources/news_lenta.csv";
  private static int len;
  private static final int testSize = 3000;
  private static final int trainSize = 10000;

  private final List<String> topics = new ArrayList<>();
  private final List<String> texts = new ArrayList<>();
  private final List<SparseVec> mx = new ArrayList<>();
  private List<Document> documents = new ArrayList<>();
  private final SklearnSgdPredictor predictor = new SklearnSgdPredictor(CNT_VECTORIZER_PATH, WEIGHTS_PATH);
  private List<String> testTopics;
  private List<String> testTexts;
  private List<SparseVec> trainingSetList;
  private SparseMx trainingSet;
  private String[] correctTopics;
  private String[] allTopics;

  @BeforeClass
  public void beforeClass() {
    predictor.init();
    allTopics = Arrays.stream(predictor.getTopics()).map(String::trim).map(String::toLowerCase).toArray(String[]::new);
  }

  @BeforeTest
  public void before() {
    mx.clear();
    documents.clear();
    topics.clear();
    texts.clear();
  }

  private Stream<TextDocument> documents() throws IOException {
    final Reader reader = new InputStreamReader(new FileInputStream(PATH_TO_DATA), Charset.forName("UTF-8"));
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
    List<TextDocument> documents = StreamSupport.stream(csvSpliterator, false).limit(testSize + trainSize).map(r -> new TextDocument(
            r.get(4), // url order
            r.get(1),
            String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
            r.get(0).trim().toLowerCase(),
            counter.incrementAndGet()
    )).filter(doc -> !doc.topic().equals("все")).filter(doc -> !doc.topic().equals("")).collect(Collectors.toList());
    Collections.reverse(documents);
    documents.sort(Comparator.comparing(TextDocument::name));
    return documents.stream();
  }

  private void calcCompleteIdf() {
    try {
      final int testCount = (int) documents().count();
      final int features = predictor.getWeights().columns();
      Map<String, Integer> idf = new HashMap<>();
      documents().forEach(doc -> {
        final String docText = doc.content();
        texts.add(docText);

        String topic = doc.topic();
        topics.add(topic);

        SklearnSgdPredictor.text2words(docText)
                .distinct()
                .filter(word -> predictor.wordIndex(word) > 0)
                .forEach(word -> idf.put(word, idf.getOrDefault(word, 0) + 1));
      });


      documents().forEach(doc -> {
        final String docText = doc.content();

        final Map<String, Long> cnt = SklearnSgdPredictor
                .text2words(docText)
                .filter(word -> predictor.wordIndex(word) > 0)
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));

        final double sum = cnt.values()
                .stream()
                .mapToLong(Long::longValue)
                .sum();
        Map<String, Double> tfIdf = new HashMap<>();
        cnt.forEach((key, value) -> {
          final double tfIdfValue = (value / sum) * Math.log(testCount / (double) idf.get(key)) + 1;
          tfIdf.put(key, tfIdfValue);
        });

        final double norm = Math.sqrt(tfIdf.values()
                .stream()
                .mapToDouble(x -> x * x)
                .sum());

        tfIdf.forEach((s, v) -> tfIdf.put(s, v / norm));

        final Document document = new Document(tfIdf);
        documents.add(document);

        SparseVec vec = new SparseVec(features);
        tfIdf.forEach((word, value) -> vec.set(predictor.wordIndex(word), value));
        mx.add(vec);
      });
      //allTopics = documents().map(TextDocument::topic).map(String::trim).map(String::toLowerCase).distinct().toArray(String[]::new);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    len = topics.size();

    testTopics = topics.stream().skip(len - testSize).collect(Collectors.toList());
    testTexts = texts.stream().skip(len - testSize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testSize).collect(Collectors.toList());
    trainingSet = new SparseMx(mx.stream().limit(trainSize).toArray(SparseVec[]::new));
    correctTopics = topics.stream().limit(trainSize).toArray(String[]::new);
  }

  private long time(String url) {
    String[] ls = url.substring(22, 31).split("/");
    int year = Integer.parseInt(ls[0]);
    int month = Integer.parseInt(ls[1]);
    int day = Integer.parseInt(ls[2]);
    Calendar c = Calendar.getInstance();
    c.set(year, month - 1, day, 0, 0);
    return c.getTime().getTime();
  }

  private void calcWindowIdf(int windowSizeDays) {
    try {

      final int testCount = (int) documents().count();
      final int features = predictor.getWeights().columns();
      Map<String, Double> idf = new HashMap<>();
      Map<String, Integer> df = new HashMap<>();

      AtomicLong currentDate = new AtomicLong(0);
      double lam = 0.99999;

      documents().forEach(doc -> {
        long date = time(doc.name());

        if (currentDate.get() == 0 || (currentDate.get() - date >  windowSizeDays * 24 * 3600000l)) {
          idf.forEach((s, v) -> idf.put(s, (v + df.getOrDefault(s, 0)) * lam));
          df.clear();
          currentDate.set(date);
        }
        final String docText = doc.content();
        texts.add(docText);

        String topic = doc.topic();
        topics.add(topic);

        SklearnSgdPredictor.text2words(docText)
                .distinct()
                .filter(word -> predictor.wordIndex(word) > 0)
                .forEach(word -> df.put(word, df.getOrDefault(word, 0) + 1));

        final Map<String, Long> cnt = SklearnSgdPredictor
                .text2words(docText)
                .filter(word -> predictor.wordIndex(word) > 0)
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));

        final double sum = cnt.values()
                .stream()
                .mapToLong(Long::longValue)
                .sum();
        Map<String, Double> tfIdf = new HashMap<>();
        cnt.forEach((key, value) -> {
          final double tfIdfValue = (value / sum) * Math.log(testCount / (idf.getOrDefault(key, 0d) + df.getOrDefault(key, 0))) + 1;
          tfIdf.put(key, tfIdfValue);
        });

        final double norm = Math.sqrt(tfIdf.values()
                .stream()
                .mapToDouble(x -> x * x)
                .sum());

        tfIdf.forEach((s, v) -> tfIdf.put(s, v / norm));

        final Document document = new Document(tfIdf);
        documents.add(document);

        SparseVec vec = new SparseVec(features);
        tfIdf.forEach((word, value) -> vec.set(predictor.wordIndex(word), value));
        mx.add(vec);
      });
      //allTopics = documents().map(TextDocument::topic).map(String::trim).map(String::toLowerCase).distinct().toArray(String[]::new);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    len = topics.size();

    testTopics = topics.stream().skip(len - testSize).collect(Collectors.toList());
    testTexts = texts.stream().skip(len - testSize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testSize).collect(Collectors.toList());
    trainingSetList = mx.stream().limit(trainSize).collect(Collectors.toList());
    correctTopics = topics.stream().limit(trainSize).toArray(String[]::new);
  }

  private void readCompleteIdf() {
    try (BufferedReader br = new BufferedReader(new FileReader(new File(PATH_TO_TEST_DATA)))) {
      final double[] data = parseDoubles(br.readLine());
      final int testCount = (int) data[0];
      final int features = (int) data[1];

      for (int i = 0; i < testCount; i++) {
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

    len = topics.size();

    testTopics = topics.stream().skip(len - testSize).collect(Collectors.toList());
    testTexts = texts.stream().skip(len - testSize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testSize).collect(Collectors.toList());
    trainingSet = new SparseMx(mx.stream().limit(trainSize).toArray(SparseVec[]::new));
    correctTopics = topics.stream().limit(trainSize).toArray(String[]::new);
  }

  private Mx startMx() {
    return new VecBasedMx(predictor.getWeights().rows(), predictor.getWeights().columns());
  }

  private double accuracy() {
    double truePositives = 0;
    for (int i = 0; i < testSize; i++) {
      String text = testTexts.get(i);
      String ans = testTopics.get(i);
      Document doc = documents.get(i);

      Topic[] prediction = predictor.predict(doc);

      Arrays.sort(prediction);
      if (ans.equals(prediction[0].name().trim().toLowerCase())) {
        truePositives++;
      }
      //LOGGER.info("Doc: {}", text);
      //LOGGER.info("Real answers: {}", ans);
      //LOGGER.info("Predict: {}", (Object) prediction);
      //LOGGER.info("\n");
    }

    double accuracy = truePositives / testSize;
    LOGGER.info("Accuracy: {}", accuracy);
    return accuracy;
  }


  @Test
  public void countingTfIdfTest() {
    readCompleteIdf();
    List<SparseVec> checkMx = new ArrayList<>();
    for (SparseVec vec: mx) {
      checkMx.add(VecTools.copySparse(vec));
    }
    mx.clear();
    calcCompleteIdf();
    for (int i = 0; i < 10; i++) {
      LOGGER.info("features 1 {}", checkMx.get(i));
      LOGGER.info("features 2 {}", mx.get(i));
    }
  }

  @Test
  public void partialFitTestCompleteIdf() {
    calcCompleteIdf();

    for (String topic: allTopics) {
      LOGGER.info("topic: {}", topic);
    }

    LOGGER.info("Updating weights");
    Optimizer optimizer = SoftmaxRegressionOptimizer
            .builder()
            .startAlpha(0.2)
            .lambda2(0.1)
            .build(allTopics);

    double kek = System.nanoTime();
    Mx newWeights = optimizer.optimizeWeights(trainingSet, correctTopics, startMx());
    kek = System.nanoTime() - kek;
    LOGGER.info("Time in nanosec: {}", kek);

    predictor.updateWeights(newWeights);

    assertTrue(accuracy() >= 0.62);
  }

  @Test
  public void partialFitTestWindowIdfBatches() {
    calcWindowIdf(1);
    final int trainBatchSizes[] = new int[]{5000, 2000, 1000, 1000, 1000};
    LOGGER.info("Updating weights");
    Optimizer optimizer = SoftmaxRegressionOptimizer
            .builder()
            .startAlpha(0.14)
            .lambda2(0.1)
            .batchSize(500)
            .build(allTopics);

    double kek = System.nanoTime();

    Mx newWeights = startMx();
    for (int i = 0, offset = 0; i < trainBatchSizes.length; offset += trainBatchSizes[i], i++) {
      final int trainBatchSize = trainBatchSizes[i];
      SparseMx trainingBatch = new SparseMx(trainingSetList.stream().skip(offset).limit(trainBatchSize).toArray(SparseVec[]::new));
      String[] correctTopicsBatch = Arrays.stream(correctTopics).skip(offset).limit(trainBatchSize).toArray(String[]::new);
      newWeights = optimizer.optimizeWeights(trainingBatch, correctTopicsBatch, newWeights);
      predictor.updateWeights(newWeights);
      accuracy();
    }
    kek = System.nanoTime() - kek;
    LOGGER.info("Time in nanosec: {}", kek);
    assertTrue(accuracy() >= 0.62);
  }

  @Test
  public void testStreamPredict() throws InterruptedException, TimeoutException {
    final List<Document> documents = new ArrayList<>();
    final List<String> texts = new ArrayList<>();
    final List<double[]> pyPredictions = new ArrayList<>();
    { //read data for test
      final SklearnSgdPredictor predictor = new SklearnSgdPredictor(CNT_VECTORIZER_PATH, WEIGHTS_PATH);
      try (BufferedReader br = new BufferedReader(new FileReader(new File(PATH_TO_TEST_DATA)))) {
        final double[] data = parseDoubles(br.readLine());
        final int testCount = (int) data[0];

        for (int i = 0; i < testCount; i++) {
          final double[] pyPrediction = parseDoubles(br.readLine());
          pyPredictions.add(pyPrediction);

          final String docText = br.readLine().toLowerCase();
          final Map<String, Double> tfIdf = new HashMap<>();
          final double[] tfidfFeatures = parseDoubles(br.readLine());
          texts.add(docText);
          SklearnSgdPredictor.text2words(docText).forEach(word -> {
            final int featureIndex = predictor.wordIndex(word);
            tfIdf.put(word, tfidfFeatures[featureIndex]);
          });
          final Document document = new Document(tfIdf);
          documents.add(document);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    final ActorSystem system = ActorSystem.create("testStand", ConfigFactory.load("remote"));
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime.Builder().parallelism(2).build()) {
      try (final FlameRuntime.Flame flame = runtime.run(predictorGraph())) {
        final List<AkkaFront.FrontHandle<Document>> handles = flame.attachFront(
                "totalOrderFront",
                new AkkaFrontType<Document>(system)
        ).collect(Collectors.toList());
        for (int i = 1; i < handles.size(); i++) {
          handles.get(i).unregister();
        }
        final AkkaFront.FrontHandle<Document> sink = handles.get(0);

        final AwaitResultConsumer<Topic[]> consumer = new AwaitResultConsumer<>(documents.size());
        flame.attachRear("totalOrderRear", new AkkaRearType<>(system, Topic[].class))
                .forEach(r -> r.addListener(consumer));
        documents.forEach(sink);
        sink.unregister();
        consumer.await(10, TimeUnit.MINUTES);

        final List<Topic[]> result = consumer.result().collect(Collectors.toList());
        Assert.assertEquals(result.size(), documents.size());

        for (int i = 0; i < result.size(); i++) {
          double maxDiff = 0;
          for (int j = 0; j < pyPredictions.get(i).length; j++) {
            final double diff = abs(pyPredictions.get(i)[j] - result.get(i)[j].probability());
            maxDiff = max(diff, maxDiff);
          }
          assertTrue(maxDiff < MathTools.EPSILON);

          Arrays.sort(result.get(i));
          LOGGER.info("Doc: {}", texts.get(i));
          LOGGER.info("Max diff {} in predictions", maxDiff);
          LOGGER.info("Predict: {}", (Object) result.get(i));
          LOGGER.info("\n");
        }
      }
    }
    Await.ready(system.terminate(), Duration.Inf());
  }

  @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
  private Graph predictorGraph() {
    final Source source = new Source();
    final Sink sink = new Sink();
    final SklearnSgdPredictor predictor = new SklearnSgdPredictor(CNT_VECTORIZER_PATH, WEIGHTS_PATH);
    final FlameMap<Document, Topic[]> classifierMap = new FlameMap<>(
            new ClassifierFunction(predictor),
            Document.class,
            new Runnable() {
              @Override
              public void run() {
                predictor.init();
              }
            }
    );
    return new Graph.Builder()
            .link(source, classifierMap)
            .link(classifierMap, sink)
            .colocate(source, classifierMap, sink)
            .build(source, sink);
  }

  private static class ClassifierFunction implements Function<Document, Stream<Topic[]>> {
    private final TopicsPredictor predictor;

    private ClassifierFunction(TopicsPredictor predictor) {
      this.predictor = predictor;
    }

    @Override
    public Stream<Topic[]> apply(Document document) {
      return Stream.of(new Topic[][]{predictor.predict(document)});
    }
  }

  private static double[] parseDoubles(String line) {
    return Arrays
            .stream(line.split(" "))
            .mapToDouble(Double::parseDouble)
            .toArray();
  }
}
