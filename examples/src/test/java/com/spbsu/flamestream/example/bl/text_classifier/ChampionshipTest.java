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
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.spbsu.flamestream.example.bl.text_classifier.FTRLProximalTest.callPython;

public class ChampionshipTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChampionshipTest.class.getName());
  private static final String WEIGHTS_PATH = "src/main/resources/classifier_weights";
  private final SklearnSgdPredictor predictor = new SklearnSgdPredictor(WEIGHTS_PATH);
  private List<DataPoint> trainingSetList = new ArrayList<>();
  private List<DataPoint> testSetList = new ArrayList<>();
  private List<Boolean> isTrain = new ArrayList<>();
  private FTRLProximal optimizer;
  private FTRLProximal warmUpOptimizer;
  private String[] allTopics;
  private static int testSize = 3100;
  private static int trainSize = 3100;
  private static int warmUp = 3100;
  private static int offset = 0;
  private static int features;

  @BeforeClass
  public void beforeClass() {
    predictor.init();
    allTopics = Arrays.stream(predictor.getTopics()).map(String::trim).map(String::toLowerCase).toArray(String[]::new);
    optimizer = FTRLProximal.builder()
            .alpha(1.8)
            .beta(0.0138)
            .lambda1(0.0062)
            .lambda2(0.010)
            .build(allTopics);
    warmUpOptimizer = FTRLProximal.builder()
            .alpha(2.4)
            .beta(0.0138)
            .lambda1(0.009)
            .lambda2(0.084)
            .build(allTopics);
  }

  private Mx startMx() {
    return new VecBasedMx(allTopics.length, features);
  }

  private static void splitDatasetWarmUpTestComplete(int seed) {
    int warmUp = 5000;
    splitDatasetWarmupTestComplete(seed, trainSize, testSize, warmUp, offset);
  }

  private static void splitDatasetWarmupTestComplete(int seed, int trainSize, int testSize, int warmUp, int offset) {
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
  }

  private void readTestTrain() {
    FTRLProximalTest.readTestTrain(trainingSetList, testSetList);
    features = FTRLProximalTest.features();
  }

  private int[] buildHistogramStreaming(FTRLProximal warmUpOptimizer, FTRLProximal optimizer) {
    int[] countClasses = new int[allTopics.length];
    ModelState state = new FTRLState(startMx());
    for (int i = 0; i < warmUp; i++) {
      state = warmUpOptimizer.step(trainingSetList.get(i), state);
    }
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
        countClasses[argmax]++;
      }
    }
    return countClasses;
  }

  private int[] buildHistogramComplete(FTRLProximal warmUpOptimizer, FTRLProximal optimizer) {
    int[] countClasses = new int[allTopics.length];
    ModelState state = new FTRLState(startMx());
    for (DataPoint dataPoint: trainingSetList) {
      state = warmUpOptimizer.step(dataPoint, state);
    }
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
      countClasses[argmax]++;
    }
    return countClasses;
  }

  private void splitDatasetWindow(int seed) {
    int warmUp = 1900;
    FTRLProximalTest.splitDatasetWindow(seed, trainSize, testSize, warmUp, offset);
  }

  private void splitDatasetNoShuffleComplete(int seed) {
    FTRLProximalTest.splitDatasetNoShuffleComplete(seed, trainSize, testSize, warmUp, offset);
  }

  @Test
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
  public void chanpionshipTest() {
    splitDatasetWindow(42);
    readStreaming();
    int[] histogramStreaming = buildHistogramStreaming(warmUpOptimizer, optimizer);
    LOGGER.info("Streaming histogram calculated");

    splitDatasetWarmUpTestComplete(42);
    readTestTrain();
    int[] histogramComplete = buildHistogramComplete(warmUpOptimizer, optimizer);
    LOGGER.info("Complete histogram calculated");

    try(PrintWriter out = new PrintWriter("src/test/resources/tmp_hist.txt")) {
      out.println(String.join(",", allTopics));
      out.println(Arrays.stream(histogramStreaming)
              .mapToObj(Integer::toString)
              .collect(Collectors.joining(" ")));
      out.println(Arrays.stream(histogramComplete)
              .mapToObj(Integer::toString)
              .collect(Collectors.joining(" ")));
      List<String> topics = Arrays.asList(allTopics);
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
}
