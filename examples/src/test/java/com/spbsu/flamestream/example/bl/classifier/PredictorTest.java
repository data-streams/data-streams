package com.spbsu.flamestream.example.bl.classifier;

import akka.actor.ActorSystem;
import com.expleague.commons.math.MathTools;
import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.impl.mx.RowsVecArrayMx;
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
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

  @Test
  public void testClassifier() throws IOException {
    final List<TextDocument> collect = documents("/home/tyoma/Downloads/news_lenta.csv").limit(20000)
            .filter(textDocument -> !textDocument.topic().equals(""))
            .filter(textDocument -> !textDocument.topic().equals("Все"))
            .collect(Collectors.toList());
    Collections.reverse(collect);

    final SklearnSgdPredictor predictor = new SklearnSgdPredictor(CNT_VECTORIZER_PATH, WEIGHTS_PATH);
    predictor.init();
    final List<Vec> xTrain = new ArrayList<>();
    final List<String> yTrain = new ArrayList<>();
    final List<Vec> xTest = new ArrayList<>();
    final List<String> yTest = new ArrayList<>();

    final Map<String, Integer> idf = new HashMap<>();
    collect.forEach(textDocument -> SklearnSgdPredictor.text2words(textDocument.content())
            .collect(Collectors.toSet())
            .forEach(s -> idf.merge(s, 1, Integer::sum)));

    final int counter[] = {0};
    collect.forEach(textDocument -> {
      counter[0] += 1;
      // update idf
      //SklearnSgdPredictor.text2words(textDocument.content())
      //        .collect(Collectors.toSet())
      //        .forEach(s -> idf.merge(s, 1, Integer::sum));


      final Map<String, Long> tf = SklearnSgdPredictor.text2words(textDocument.content())
              .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
      final Map<String, Double> tfIdf = new HashMap<>();
      { //normalized tf-idf
        double squareSum = 0.0;
        for (String word : tf.keySet()) {
          double tfIdfValue = tf.get(word) * Math.log((double) collect.size() / (double) idf.get(word)) + 1;
          squareSum += (tfIdfValue * tfIdfValue);
          tfIdf.put(word, tfIdfValue);
        }
        final double norm = Math.sqrt(squareSum);
        tfIdf.forEach((s, v) -> tfIdf.put(s, v / norm));
      }

      final Vec vec = new SparseVec(predictor.getWeights().columns());
      tfIdf.forEach((s, val) -> {
        final int index = predictor.wordIndex(s);
        if (index != -1) {
          vec.set(index, val);
        }
      });

      if (counter[0] % 3 == 0) {
        xTest.add(vec);
        yTest.add(textDocument.topic());
      } else {
        xTrain.add(vec);
        yTrain.add(textDocument.topic());
      }
    });

    final Optimizer optimizer = new SoftmaxRegressionOptimizer(predictor.getTopics());
    final Mx prev = new SparseMx(predictor.getWeights().rows(), predictor.getWeights().columns());
    final Mx weights = optimizer.optimizeWeights(
            new RowsVecArrayMx(xTrain.toArray(new Vec[xTrain.size()])),
            yTrain.toArray(new String[yTrain.size()]),
            prev
    );

    predictor.updateWeights(weights);
    double truePositives = 0;
    for (int i = 0; i < xTest.size(); i++) {
      Topic[] prediction = predictor.predict(xTest.get(i));
      Arrays.sort(prediction);
      if (yTest.get(i).equals(prediction[0].name().trim())) {
        truePositives++;
      }
      //LOGGER.info("Doc: {}", text);
      //LOGGER.info("Real answers: {}", ans);
      //LOGGER.info("Predict: {}", (Object) prediction);
      //LOGGER.info("\n");
    }

    double accuracy = truePositives / xTest.size();
    LOGGER.info("Accuracy: {}", accuracy);
  }

  @Test
  public void partialFitTest() {
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
    final int testSize = 1000;

    List<String> testTopics = topics.stream().skip(len - testSize).collect(Collectors.toList());
    List<String> testTexts = texts.stream().skip(len - testSize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testSize).collect(Collectors.toList());

    SparseMx trainingSet = new SparseMx(mx.stream().limit(len - testSize).toArray(SparseVec[]::new));
    LOGGER.info("Updating weights");
    Optimizer optimizer = new SoftmaxRegressionOptimizer(predictor.getTopics());
    String[] correctTopics = topics.stream().limit(len - testSize).toArray(String[]::new);
    Mx newWeights = optimizer.optimizeWeights(trainingSet, correctTopics, predictor.getWeights());
    predictor.updateWeights(newWeights);

    double truePositives = 0;
    for (int i = 0; i < testSize; i++) {
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

    double accuracy = truePositives / testSize;
    LOGGER.info("Accuracy: {}", accuracy);
    assertTrue(accuracy >= 0.62);
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

  private Stream<TextDocument> documents(String path) throws IOException {
    final Reader reader = new InputStreamReader(new FileInputStream(path));
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
      String recordText = r.get(1); // text order
      Matcher m = p.matcher(recordText);
      StringBuilder text = new StringBuilder();
      while (m.find()) {
        text.append(" ");
        text.append(m.group());
      }
      return new TextDocument(
              r.get(4), // url order
              text.substring(1).toLowerCase(),
              String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
              counter.incrementAndGet(),
              r.get(0)
      );
    });
  }
}
