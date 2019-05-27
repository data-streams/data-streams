package com.spbsu.flamestream.example.bl.text_classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecIterator;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.ArrayVec;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLProximal;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLState;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.spbsu.flamestream.example.bl.text_classifier.FTRLProximalTest.CNT_VECTORIZER;
import static com.spbsu.flamestream.example.bl.text_classifier.FTRLProximalTest.callPython;

public class ChampionshipTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChampionshipTest.class.getName());
  private static final String WEIGHTS_PATH = "src/main/resources/classifier_weights";
  private final SklearnSgdPredictor predictor = new SklearnSgdPredictor(WEIGHTS_PATH);
  private List<DataPoint> trainingSetList = new ArrayList<>();
  private List<DataPoint> testSetList = new ArrayList<>();
  private final List<String> trackingWords = Arrays.asList("франции", "хорватии", "аргентины", "англии", "гол");
  private final List<Integer> trackingIndices = new ArrayList<>();
  private List<Boolean> isTrain = new ArrayList<>();
  private FTRLProximal optimizer;
  private FTRLProximal warmUpOptimizer;
  private String[] allTopics;
  private static int chmPartSize = 4000;
  private static int testSize = chmPartSize;
  private static int trainSize = chmPartSize;
  private static int warmUp = 2 * chmPartSize;
  private static int streamingWarmUp = chmPartSize;
  private static int offset = 0;
  private static int features;

  @BeforeClass
  public void beforeClass() {
    predictor.init();
    allTopics = Arrays.stream(predictor.getTopics()).map(String::trim).map(String::toLowerCase).toArray(String[]::new);
    optimizer = FTRLProximal.builder()
            .alpha(1.0)
            .beta(0.0138)
            .lambda1(0.0062)
            .lambda2(0.010)
            .build(allTopics);
    warmUpOptimizer = FTRLProximal.builder()
            .alpha(2.3)
            .beta(0.0138)
            .lambda1(0.009)
            .lambda2(0.084)
            .build(allTopics);
  }

  private Mx startMx() {
    return new VecBasedMx(allTopics.length, features);
  }

  private void splitDatasetWarmUpTestComplete(int seed) {
    splitDatasetWarmUpTestComplete(seed, trainSize, testSize, warmUp, offset);
  }

  private void splitDatasetWarmUpTestComplete(int seed, int trainSize, int testSize, int warmUp, int offset) {
    String pythonCommand = String.format(
            Locale.US,
            "sklearn_split_dataset.py %d %d %d %d %d %s",
            trainSize,
            testSize,
            warmUp,
            offset,
            seed,
            "warmup_test_complete"
    );
    LOGGER.info(callPython(pythonCommand));
  }

  private void readStreaming() {
    FTRLProximalTest.readStreaming(trainingSetList, isTrain);
    features = FTRLProximalTest.features();

    trackingIndices.clear();
    for (int i = 0; i < trackingWords.size(); i++) {
      trackingIndices.add(0);
    }
    try {
      Files.lines(new File(CNT_VECTORIZER).toPath()).forEach(line -> {
        String[] pair = line.split(" ");
        int wordIndex = trackingWords.indexOf(pair[0]);
        if (wordIndex != -1) {
          trackingIndices.set(wordIndex, Integer.parseInt(pair[1]));
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void readTestTrain() {
    FTRLProximalTest.readTestTrain(trainingSetList, testSetList);
    features = FTRLProximalTest.features();
  }

  private int[] buildHistogramStreaming(FTRLProximal warmUpOptimizer, FTRLProximal optimizer, int realWarmUp) {
    final List<List<Double>> trackingList = new ArrayList<>(trackingWords.size());
    for (int ind = 0; ind < trackingWords.size(); ind++) {
      trackingList.add(new ArrayList<>());
    }

    final int topicIndex = Arrays.asList(allTopics).indexOf("футбол");

    int[] countClasses = new int[allTopics.length];
    ModelState state = new FTRLState(startMx());
    for (int i = warmUp - realWarmUp; i < warmUp; i++) {
      state = warmUpOptimizer.step(trainingSetList.get(i), state);
      for (int ind = 0; ind < trackingList.size(); ind++) {
        trackingList.get(ind).add(state.weights().get(topicIndex, trackingIndices.get(ind)));
      }
    }
    int count = 0;
    int correct = 0;
    for (int i = warmUp; i < trainingSetList.size(); i++) {
      if (isTrain.get(i)) {
        state = optimizer.step(trainingSetList.get(i), state);
        for (int ind = 0; ind < trackingList.size(); ind++) {
          trackingList.get(ind).add(state.weights().get(topicIndex, trackingIndices.get(ind)));
        }
      } else {
        Mx weights = state.weights();
        Vec x = trainingSetList.get(i).getFeatures();
        Vec p = new ArrayVec(weights.rows());
        for (int j = 0; j < weights.rows(); j++) {
          VecIterator xNz = x.nonZeroes();
          while (xNz.advance()) {
            p.adjust(j, xNz.value() * weights.get(j, xNz.index()));
          }
        }
        int argmax = VecTools.argmax(p);
        count++;
        if (allTopics[argmax].equals(trainingSetList.get(i).getLabel())) {
          correct++;
        }
        countClasses[argmax]++;
      }
    }
    LOGGER.info("Online accuracy: {}", correct / (double) count);

    try(PrintWriter writer = new PrintWriter("src/test/resources/tmp.txt")) {
      for (int i = 0; i < trackingList.size(); i++) {
        writer.print(trackingWords.get(i));
        writer.print(',');
        writer.println(trackingList.get(i).stream()
                .map(Object::toString)
                .collect(Collectors.joining(",")));
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    return countClasses;
  }

  private int[] buildHistogramComplete(FTRLProximal optimizer, int realWarmUp) {
    int[] countClasses = new int[allTopics.length];
    ModelState state = new FTRLState(startMx());
    for (int i = warmUp - realWarmUp; i < trainingSetList.size(); i++) {
      state = optimizer.step(trainingSetList.get(i), state);
    }
    int count = 0;
    int correct = 0;
    for (DataPoint dataPoint: testSetList) {
      Mx weights = state.weights();
      Vec x = dataPoint.getFeatures();
      Vec p = new ArrayVec(weights.rows());
      for (int j = 0; j < weights.rows(); j++) {
        VecIterator xNz = x.nonZeroes();
        while (xNz.advance()) {
          p.adjust(j, xNz.value() * weights.get(j, xNz.index()));
        }
      }
      int argmax = VecTools.argmax(p);
      count++;
      if (allTopics[argmax].equals(dataPoint.getLabel())) {
        correct++;
      }
      countClasses[argmax]++;
    }
    LOGGER.info("Offline accuracy: {}", correct / (double) count);
    return countClasses;
  }

  private void splitDatasetWindow(int seed) {
    FTRLProximalTest.splitDatasetWindow(seed, trainSize, testSize, warmUp, offset);
  }

  private void splitDatasetNoShuffleComplete(int seed) {
    FTRLProximalTest.splitDatasetNoShuffleComplete(seed, trainSize, testSize, warmUp, offset);
  }

  @Test
  public void championshipTest() {
    FTRLProximalTest.splitDatasetWindow(42, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
    readStreaming();
    int[] histogramStreaming = buildHistogramStreaming(warmUpOptimizer, optimizer, chmPartSize);
    LOGGER.info("Streaming histogram calculated");

    splitDatasetWarmUpTestComplete(42, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
    readTestTrain();
    int[] histogramComplete = buildHistogramComplete(warmUpOptimizer, 2 * chmPartSize);
    LOGGER.info("Complete histogram calculated");

    try(PrintWriter out = new PrintWriter("src/test/resources/tmp_hist.txt")) {
      out.println(String.join(",", allTopics));
      out.println(Arrays.stream(histogramComplete)
              .mapToObj(Integer::toString)
              .collect(Collectors.joining(" ")));
      out.println(Arrays.stream(histogramStreaming)
              .mapToObj(Integer::toString)
              .collect(Collectors.joining(" ")));
      Map<String, Long> counts = testSetList.stream()
              .collect(Collectors.groupingBy(
                      DataPoint::getLabel,
                      Collectors.counting()
              ));
      out.println(Arrays.stream(allTopics)
              .map(topic -> counts.containsKey(topic) ? counts.get(topic).toString() : "0")
              .collect(Collectors.joining(" ")));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  private double accuracyStreaming(FTRLProximal warmUpOptimizer, FTRLProximal optimizer) {
    return accuracyStreaming(warmUpOptimizer, optimizer, streamingWarmUp);
  }

  private double accuracyStreaming(FTRLProximal warmUpOptimizer, FTRLProximal optimizer, int streamingWarmUp) {
    ModelState state = new FTRLState(startMx());
    for (int i = warmUp - streamingWarmUp; i < warmUp; i++) {
      state = warmUpOptimizer.step(trainingSetList.get(i), state);
    }
    int count = 0;
    int correct = 0;
    for (int i = warmUp; i < trainingSetList.size(); i++) {
      if (isTrain.get(i)) {
        state = optimizer.step(trainingSetList.get(i), state);
      } else {
        Mx weights = state.weights();
        Vec x = trainingSetList.get(i).getFeatures();
        Vec p = new ArrayVec(weights.rows());
        for (int j = 0; j < weights.rows(); j++) {
          VecIterator xNz = x.nonZeroes();
          while (xNz.advance()) {
            p.adjust(j, xNz.value() * weights.get(j, xNz.index()));
          }
        }
        int argmax = VecTools.argmax(p);
        count++;
        if (allTopics[argmax].equals(trainingSetList.get(i).getLabel())) {
          correct++;
        }
      }
    }
    LOGGER.info("Online accuracy: {}", correct / (double) count);
    return correct / (double) count;
  }

  private double accuracyDegradedStreaming(FTRLProximal warmUpOptimizer, FTRLProximal optimizer, int seed) {
    return accuracyDegradedStreaming(warmUpOptimizer, optimizer, seed, streamingWarmUp);
  }

  private double accuracyDegradedStreaming(FTRLProximal warmUpOptimizer, FTRLProximal optimizer, int seed, int streamingWarmUp) {
    ArrayList<Boolean> isDegraded = new ArrayList<>(features);
    for (int i = 0; i < features / 2; i++) {
      isDegraded.add(false);
    }
    for (int i = features / 2; i < features; i++) {
      isDegraded.add(true);
    }
    Collections.shuffle(isDegraded, new Random(seed));
    ModelState state = new FTRLState(startMx());
    for (int i = warmUp - streamingWarmUp; i < warmUp; i++) {
      Vec x = VecTools.copySparse(trainingSetList.get(i).getFeatures());
      for (VecIterator iterator = x.nonZeroes(); iterator.advance();) {
        if (isDegraded.get(iterator.index())) {
          iterator.setValue(0);
        }
      }
      state = warmUpOptimizer.step(new DataPoint(x, trainingSetList.get(i).getLabel()), state);
    }
    int count = 0;
    int correct = 0;
    for (int i = warmUp; i < trainingSetList.size(); i++) {
      Vec x = VecTools.copySparse(trainingSetList.get(i).getFeatures());
      for (VecIterator iterator = x.nonZeroes(); iterator.advance();) {
        if (isDegraded.get(iterator.index())) {
          iterator.setValue(0);
        }
      }
      if (isTrain.get(i)) {
        state = optimizer.step(new DataPoint(x, trainingSetList.get(i).getLabel()), state);
      } else {
        Mx weights = state.weights();
        Vec p = new ArrayVec(weights.rows());
        for (int j = 0; j < weights.rows(); j++) {
          VecIterator xNz = x.nonZeroes();
          while (xNz.advance()) {
            p.adjust(j, xNz.value() * weights.get(j, xNz.index()));
          }
        }
        int argmax = VecTools.argmax(p);
        count++;
        if (allTopics[argmax].equals(trainingSetList.get(i).getLabel())) {
          correct++;
        }
      }
    }
    LOGGER.info("Online accuracy: {}", correct / (double) count);
    return correct / (double) count;
  }

  private double accuracyComplete(FTRLProximal optimizer) {
    return accuracyComplete(optimizer, warmUp);
  }

  private double accuracyComplete(FTRLProximal optimizer, int realWarmUp) {
    ModelState state = new FTRLState(startMx());
    for (int i = warmUp - realWarmUp; i < trainingSetList.size(); i++) {
      state = optimizer.step(trainingSetList.get(i), state);
    }
    int count = 0;
    int correct = 0;
    for (DataPoint dataPoint: testSetList) {
      Mx weights = state.weights();
      Vec x = dataPoint.getFeatures();
      Vec p = new ArrayVec(weights.rows());
      for (int j = 0; j < weights.rows(); j++) {
        VecIterator xNz = x.nonZeroes();
        while (xNz.advance()) {
          p.adjust(j, xNz.value() * weights.get(j, xNz.index()));
        }
      }
      int argmax = VecTools.argmax(p);
      count++;
      if (allTopics[argmax].equals(dataPoint.getLabel())) {
        correct++;
      }
    }
    LOGGER.info("Offline accuracy: {}", correct / (double) count);
    return correct / (double) count;
  }

  private double accuracyDegradedComplete(FTRLProximal optimizer, int seed) {
    return accuracyDegradedComplete(optimizer, seed, warmUp);
  }

  private double accuracyDegradedComplete(FTRLProximal optimizer, int seed, int realWarmUp) {
    ModelState state = new FTRLState(startMx());
    ArrayList<Boolean> isDegraded = new ArrayList<>(features);
    for (int i = 0; i < features / 2; i++) {
      isDegraded.add(false);
    }
    for (int i = features / 2; i < features; i++) {
      isDegraded.add(true);
    }
    Collections.shuffle(isDegraded, new Random(seed));

    for (int i = warmUp - realWarmUp; i < trainingSetList.size(); i++) {
      Vec x = VecTools.copySparse(trainingSetList.get(i).getFeatures());
      for (VecIterator iterator = x.nonZeroes(); iterator.advance();) {
        if (isDegraded.get(iterator.index())) {
          iterator.setValue(0);
        }
      }
      state = optimizer.step(new DataPoint(x, trainingSetList.get(i).getLabel()), state);
    }
    int count = 0;
    int correct = 0;
    for (DataPoint dataPoint: testSetList) {
      Mx weights = state.weights();
      Vec x = VecTools.copySparse(dataPoint.getFeatures());
      for (VecIterator iterator = x.nonZeroes(); iterator.advance();) {
        if (isDegraded.get(iterator.index())) {
          iterator.setValue(0);
        }
      }
      Vec p = new ArrayVec(weights.rows());
      for (int j = 0; j < weights.rows(); j++) {
        VecIterator xNz = x.nonZeroes();
        while (xNz.advance()) {
          p.adjust(j, xNz.value() * weights.get(j, xNz.index()));
        }
      }
      int argmax = VecTools.argmax(p);
      count++;
      if (allTopics[argmax].equals(dataPoint.getLabel())) {
        correct++;
      }
    }
    LOGGER.info("Offline accuracy: {}", correct / (double) count);
    return correct / (double) count;
  }

  @Test
  public void testSplitSameTestDocs() {
    Random random = new Random(42);
    int sampleSize = 100;
    double streamingAccuracySample[] = new double[sampleSize];
    double completeAccuracySample[] = new double[sampleSize];
    for (int i = 0; i < 10; i++) {
      int seed = random.nextInt(Integer.MAX_VALUE);
      FTRLProximalTest.splitDatasetWindow(seed, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
      readStreaming();
      Random featureRandom = new Random(42);
      for (int j = 0; j < sampleSize; j++) {
        int featureSeed = featureRandom.nextInt(Integer.MAX_VALUE);
        streamingAccuracySample[j] += accuracyDegradedStreaming(warmUpOptimizer, optimizer, featureSeed, chmPartSize);
      }

      splitDatasetWarmUpTestComplete(seed, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
      readTestTrain();
      featureRandom = new Random(42);
      for (int j = 0; j < sampleSize; j++) {
        int featureSeed = featureRandom.nextInt(Integer.MAX_VALUE);
        completeAccuracySample[j] += accuracyDegradedComplete(warmUpOptimizer, featureSeed, 2 * chmPartSize);
      }
    }
    for (int j = 0; j < sampleSize; j++) {
      streamingAccuracySample[j] /= 10;
      completeAccuracySample[j] /= 10;
    }
    try (PrintWriter printer = new PrintWriter("same_docs.txt")) {
      printer.println(Arrays.stream(streamingAccuracySample)
              .mapToObj(Double::toString)
              .collect(Collectors
                      .joining(",", "[", "]")));
      printer.println(Arrays.stream(completeAccuracySample)
              .mapToObj(Double::toString)
              .collect(Collectors
                      .joining(",", "[", "]")));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testSplitDifferentTestDocs() {
    Random random = new Random(42);
    int sampleSize = 100;
    double streamingAccuracySample[] = new double[sampleSize];
    double completeAccuracySample[] = new double[sampleSize];
    for (int i = 0; i < 10; i++) {
      int seed = random.nextInt(Integer.MAX_VALUE);
      FTRLProximalTest.splitDatasetWindow(seed, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
      readStreaming();
      Random featureRandom = new Random(42);
      for (int j = 0; j < sampleSize; j++) {
        int featureSeed = featureRandom.nextInt(Integer.MAX_VALUE);
        streamingAccuracySample[j] += accuracyDegradedStreaming(warmUpOptimizer, optimizer, featureSeed, chmPartSize);
      }

      FTRLProximalTest.splitDatasetNoShuffleComplete(seed, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
      readTestTrain();
      featureRandom = new Random(42);
      for (int j = 0; j < sampleSize; j++) {
        int featureSeed = featureRandom.nextInt(Integer.MAX_VALUE);
        completeAccuracySample[j] += accuracyDegradedComplete(warmUpOptimizer, featureSeed, chmPartSize);
      }
    }
    for (int j = 0; j < sampleSize; j++) {
      streamingAccuracySample[j] /= 10;
      completeAccuracySample[j] /= 10;
    }
    try (PrintWriter printer = new PrintWriter("different_docs.txt")) {
      printer.println(Arrays.stream(streamingAccuracySample)
              .mapToObj(Double::toString)
              .collect(Collectors
                      .joining(",", "[", "]")));
      printer.println(Arrays.stream(completeAccuracySample)
              .mapToObj(Double::toString)
              .collect(Collectors
                      .joining(",", "[", "]")));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  //@Test
  void months() throws IOException {
    try (
            final FileInputStream in =
                    new FileInputStream("src/test/resources/news_lenta.csv");
    ) {
      AtomicInteger may = new AtomicInteger(0);
      AtomicInteger june = new AtomicInteger(0);
      AtomicInteger july = new AtomicInteger(0);
      StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              new CSVParser(new BufferedReader(new InputStreamReader(
                      in,
                      StandardCharsets.UTF_8
              )), CSVFormat.DEFAULT.withFirstRecordAsHeader()).iterator(),
              Spliterator.IMMUTABLE
      ), false)
              .filter(csvRecord -> !csvRecord.get(0).equals("Все") || !csvRecord.get(0).equals(""))
              .forEach(csvRecord -> {
                if (csvRecord.get(4).contains("/2018/05")) may.getAndIncrement();
                if (csvRecord.get(4).contains("/2018/06")) june.getAndIncrement();
                if (csvRecord.get(4).contains("/2018/07")) july.getAndIncrement();
              });
      LOGGER.info("may: {}, june: {}, july: {}", may.get(), june.get(), july.get());
    }
  }

  @Test
  public void testSplitSameTestDocsNoDropOut() {
    Random random = new Random(42);
    double streamingAccuracy = 0;
    double completeAccuracy = 0;
    for (int i = 0; i < 10; i++) {
      int seed = random.nextInt(Integer.MAX_VALUE);

      FTRLProximalTest.splitDatasetWindow(seed, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
      readStreaming();
      streamingAccuracy += accuracyStreaming(warmUpOptimizer, optimizer, chmPartSize);

      splitDatasetWarmUpTestComplete(seed, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
      readTestTrain();
      completeAccuracy += accuracyComplete(warmUpOptimizer);
    }
    streamingAccuracy /= 10;
    completeAccuracy /= 10;
    LOGGER.info("Streaming: {}, complete: {}", streamingAccuracy, completeAccuracy);
  }

  @Test
  public void testSplitDifferentTestDocsNoDropOut() {
    Random random = new Random(42);
    double streamingAccuracy = 0;
    double completeAccuracy = 0;
    for (int i = 0; i < 10; i++) {
      int seed = random.nextInt(Integer.MAX_VALUE);

      FTRLProximalTest.splitDatasetWindow(seed, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
      readStreaming();
      streamingAccuracy += accuracyStreaming(warmUpOptimizer, optimizer, chmPartSize);

      FTRLProximalTest.splitDatasetNoShuffleComplete(seed, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
      readTestTrain();
      completeAccuracy += accuracyComplete(warmUpOptimizer, chmPartSize);
    }
    streamingAccuracy /= 10;
    completeAccuracy /= 10;
    LOGGER.info("Streaming: {}, complete: {}", streamingAccuracy, completeAccuracy);
  }

  //@Test
  public void degradedTest() {
    FTRLProximalTest.splitDatasetNoShuffleComplete(42, chmPartSize, chmPartSize, 2 * chmPartSize, 0);
    readTestTrain();
    Random featureRandom = new Random(42);
    int featureSeed = featureRandom.nextInt(Integer.MAX_VALUE);
    accuracyDegradedComplete(warmUpOptimizer, featureSeed, chmPartSize);
  }
}
