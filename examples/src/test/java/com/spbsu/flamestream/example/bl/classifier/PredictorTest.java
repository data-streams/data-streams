package com.spbsu.flamestream.example.bl.classifier;

import akka.actor.ActorSystem;
import com.expleague.commons.math.MathTools;
import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxIterator;
import com.expleague.commons.math.vectors.VecIterator;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
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
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TIntArrayList;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static org.testng.Assert.assertTrue;

public class PredictorTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PredictorTest.class.getName());
  private static final String CNT_VECTORIZER_PATH = "src/main/resources/cnt_vectorizer";
  private static final String WEIGHTS_PATH = "src/main/resources/classifier_weights";
  private static final String PATH_TO_TEST_DATA = "src/test/resources/sklearn_prediction";

  @Test
  public void testWithPreviousWeights() {
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
    final int testSize = 3000;
    final int trainSize = 10000;
    final int trainBatchSize = 1000;

    List<String> testTopics = topics.stream().skip(len - testSize).collect(Collectors.toList());
    List<String> testTexts = texts.stream().skip(len - testSize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testSize).collect(Collectors.toList());



    LOGGER.info("Updating weights");
    Optimizer optimizer = SoftmaxRegressionOptimizer
            .builder()
            .startAlpha(0.05)
            .lambda2(0.1)
            .batchSize(trainBatchSize)
            .build(predictor.getTopics());

    double kek = System.nanoTime();
    int rows = predictor.getWeights().rows();
    int columns = predictor.getWeights().columns();
    Mx prevWeights = new VecBasedMx(rows, columns);
    MxIterator iterator = prevWeights.nonZeroes();
    while (iterator.advance()) {
      prevWeights.set(iterator.row(), iterator.column(), iterator.value());
    }

    Mx newWeights = prevWeights;
    /*for (int offset = 0; offset < trainSize; offset += trainBatchSize) {
      SparseMx trainingBatch = new SparseMx(mx.stream().skip(offset).limit(trainBatchSize).toArray(SparseVec[]::new));
      String[] correctTopics = topics.stream().limit(trainSize).toArray(String[]::new);
      newWeights = optimizer.optimizeWeights(trainingBatch, correctTopics, newWeights);
    }*/
    kek = System.nanoTime() - kek;
    LOGGER.info("Time in nanosec: {}", kek);

    //Mx newWeights = optimizer.optimizeWeights(partialFitSet, partialFitCorrectTopics, prevWeights);

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
  public void partialFitTestWithPreviousWeightsBatches() {
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
    final int testSize = 3000;
    final int trainSize = 10000;
    final int trainBatchSize = 1000;

    List<String> testTopics = topics.stream().skip(len - testSize).collect(Collectors.toList());
    List<String> testTexts = texts.stream().skip(len - testSize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testSize).collect(Collectors.toList());



    LOGGER.info("Updating weights");
    Optimizer optimizer = SoftmaxRegressionOptimizer
            .builder()
            .startAlpha(0.05)
            .lambda2(0.1)
            .batchSize(trainBatchSize)
            .build(predictor.getTopics());

    double kek = System.nanoTime();
    int rows = predictor.getWeights().rows();
    int columns = predictor.getWeights().columns();
    Mx prevWeights = new VecBasedMx(rows, columns);
    MxIterator iterator = prevWeights.nonZeroes();
    while (iterator.advance()) {
      prevWeights.set(iterator.row(), iterator.column(), iterator.value());
    }

    Mx newWeights = prevWeights;
    for (int offset = 0; offset < trainSize; offset += trainBatchSize) {
      SparseMx trainingBatch = new SparseMx(mx.stream().skip(offset).limit(trainBatchSize).toArray(SparseVec[]::new));
      String[] correctTopics = topics.stream().limit(trainSize).toArray(String[]::new);
      newWeights = optimizer.optimizeWeights(trainingBatch, correctTopics, newWeights);
    }
    kek = System.nanoTime() - kek;
    LOGGER.info("Time in nanosec: {}", kek);

    //Mx newWeights = optimizer.optimizeWeights(partialFitSet, partialFitCorrectTopics, prevWeights);

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
  public void partialFitTestWithPreviousWeights() {
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
    final int testSize = 3000;
    final int trainSize = 10000;

    List<String> testTopics = topics.stream().skip(len - testSize).collect(Collectors.toList());
    List<String> testTexts = texts.stream().skip(len - testSize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testSize).collect(Collectors.toList());



    LOGGER.info("Updating weights");
    Optimizer optimizer = SoftmaxRegressionOptimizer
            .builder()
            .startAlpha(0.2)
            .lambda2(0.1)
            .build(predictor.getTopics());
    SparseMx trainingSet = new SparseMx(mx.stream().limit(trainSize).toArray(SparseVec[]::new));
    String[] correctTopics = topics.stream().limit(trainSize).toArray(String[]::new);

    double kek = System.nanoTime();
    int rows = predictor.getWeights().rows();
    int columns = predictor.getWeights().columns();
    Mx prevWeights = new VecBasedMx(rows, columns);
    MxIterator iterator = prevWeights.nonZeroes();
    while (iterator.advance()) {
      prevWeights.set(iterator.row(), iterator.column(), iterator.value());
    }
    Mx newWeights = optimizer.optimizeWeights(trainingSet, correctTopics, prevWeights);
    kek = System.nanoTime() - kek;
    LOGGER.info("Time in nanosec: {}", kek);

    //Mx newWeights = optimizer.optimizeWeights(partialFitSet, partialFitCorrectTopics, prevWeights);

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
  public void partialFitTestWithoutPreviousWeights() {
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
    final int testSize = 3000;
    final int trainSize = 10000;

    List<String> testTopics = topics.stream().skip(len - testSize).collect(Collectors.toList());
    List<String> testTexts = texts.stream().skip(len - testSize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testSize).collect(Collectors.toList());



    LOGGER.info("Updating weights");
    Optimizer optimizer = SoftmaxRegressionOptimizer
            .builder()
            .startAlpha(0.2)
            .lambda2(0.1)
            .build(predictor.getTopics());
    SparseMx trainingSet = new SparseMx(mx.stream().limit(trainSize).toArray(SparseVec[]::new));
    String[] correctTopics = topics.stream().limit(trainSize).toArray(String[]::new);

    double kek = System.nanoTime();
    int rows = predictor.getWeights().rows();
    int columns = predictor.getWeights().columns();
    Mx prevWeights = new VecBasedMx(rows, columns);
    Mx newWeights = optimizer.optimizeWeights(trainingSet, correctTopics, prevWeights);
    kek = System.nanoTime() - kek;
    LOGGER.info("Time in nanosec: {}", kek);

    //Mx newWeights = optimizer.optimizeWeights(partialFitSet, partialFitCorrectTopics, prevWeights);

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
}
