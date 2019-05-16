package com.spbsu.flamestream.example.bl.text_classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecIterator;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.ArrayVec;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLProximal;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.concurrent.java8.FuturesConvertersImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

//@Test(enabled = false)
public class FTRLProximalTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FTRLProximalTest.class.getName());
  private static final String WEIGHTS_PATH = "src/main/resources/classifier_weights";
  private static final String TMP_TRAIN_PATH = "src/test/resources/tmp_train";
  private static final String TMP_TEST_PATH = "src/test/resources/tmp_test";
  private static int testSize;
  private static int trainSize;
  private static int offset;
  private static int features;

  private final SklearnSgdPredictor predictor = new SklearnSgdPredictor(WEIGHTS_PATH);
  private final List<DataPoint> trainingSetList = new ArrayList<>();
  private final List<DataPoint> testSetList = new ArrayList<>();
  private Boolean[] isTrain;
  private String[] allTopics;
  private FTRLProximal optimizer;
  private FTRLProximal warmUpOptimizer;

  @BeforeClass
  public void beforeClass() {
    testSize = 5000;
    trainSize = 10000;
    offset = 5000;
    predictor.init();
    allTopics = Arrays.stream(predictor.getTopics()).map(String::trim).map(String::toLowerCase).toArray(String[]::new);
    optimizer = FTRLProximal.builder()
            .alpha(1)
            .beta(0.0138)
            .lambda1(0.0067)
            .lambda2(0.050)
            .build(allTopics);
    warmUpOptimizer = FTRLProximal.builder()
            .alpha(1)
            .beta(0.000005)
            .lambda1(0.0067)
            .lambda2(0.125)
            .build(allTopics);
  }

  @AfterClass
  public void afterClass() {
    //noinspection ResultOfMethodCallIgnored
    //new File(TMP_TEST_PATH).delete();
    //noinspection ResultOfMethodCallIgnored
    //new File(TMP_TRAIN_PATH).delete();
  }

  private String callPython(String pythonCommand) {
    String[] commands = new String[]{
            "bash",
            "-c",
            "source ~/.bashrc; cd src/test/resources/; python " + pythonCommand
    };

    try {
      Process proc = Runtime.getRuntime().exec(commands);
      proc.waitFor();
      Scanner reader = new Scanner(new InputStreamReader(proc.getErrorStream()));
      while (reader.hasNextLine()) {
        LOGGER.error(reader.nextLine());
      }
      reader = new Scanner(new InputStreamReader(proc.getInputStream()));
      StringBuilder builder = new StringBuilder();
      while (reader.hasNextLine()) {
        builder.append(reader.nextLine());
        builder.append(System.lineSeparator());
      }
      return builder.toString();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      return "";
    }
  }

  @Test
  public void split() {
    splitDatsetWindow(42);
  }

  private void splitDatsetWindow(int seed) {
    splitDatsetWindow(seed, 12, 0.8);
  }

  private void splitDatsetWindow(int seed, double windowSize, double lam) {
    String pythonCommand = String.format(
            Locale.US,
            "sklearn_split_dataset.py %d %d %d %d %f %f %s",
            trainSize,
            testSize,
            offset,
            seed,
            windowSize,
            lam,
            "window"
    );
    LOGGER.info(callPython(pythonCommand));
  }

  private void splitDatsetRandomComplete(int seed) {
    String pythonCommand = String.format(
            Locale.US,
            "sklearn_split_dataset.py %d %d %d %d %d %f %s",
            trainSize,
            testSize,
            1000,
            seed,
            3,
            0.09,
            "random_complete"
    );
    LOGGER.info(callPython(pythonCommand));
  }

  private void splitDatsetOrderedComplete(int seed) {
    String pythonCommand = String.format(
            Locale.US,
            "sklearn_split_dataset.py %d %d %d %d %d %f %s",
            trainSize,
            testSize,
            1000,
            seed,
            3,
            0.09,
            "ordered_complete"
    );
    LOGGER.info(callPython(pythonCommand));
  }

  private void readDatasetFromFileToList(String filename, List<DataPoint> dataset) throws IOException {
    final String[] trainInfo = Files.lines(new File(filename).toPath())
            .limit(1).toArray(String[]::new)[0].split(" ");
    int size = Integer.parseInt(trainInfo[0]);
    Files.lines(new File(filename).toPath())
            .skip(1)
            .limit(size)
            .forEach(line -> {
              final String[] tokens = line.split(",");
              String topic = tokens[0].trim().toLowerCase();
              final double[] info = Arrays
                      .stream(tokens)
                      .skip(1)
                      .mapToDouble(Double::parseDouble)
                      .toArray();
              final int[] indeces = new int[info.length / 2];
              final double[] values = new double[info.length / 2];
              for (int k = 0; k < info.length; k += 2) {
                final int index = (int) info[k];
                final double value = info[k + 1];

                indeces[k / 2] = index;
                values[k / 2] = value;
              }
              dataset.add(new DataPoint(new SparseVec(features, indeces, values), topic));
            });
  }

  private void readTestTrain() {
    trainingSetList.clear();
    testSetList.clear();
    try {
      final String[] trainInfo = Files.lines(new File(TMP_TRAIN_PATH).toPath())
              .limit(1).toArray(String[]::new)[0].split(" ");
      trainSize = Integer.parseInt(trainInfo[0]);
      features = Integer.parseInt(trainInfo[1]);
      readDatasetFromFileToList(TMP_TRAIN_PATH, trainingSetList);

      readDatasetFromFileToList(TMP_TEST_PATH, testSetList);
      testSize = testSetList.size();

      LOGGER.info("Test size: {}, train size: {}", testSize, trainSize);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void readStreaming() {
    trainingSetList.clear();
    testSetList.clear();
    try {
      final String[] trainInfo = Files.lines(new File(TMP_TRAIN_PATH).toPath())
              .limit(1).toArray(String[]::new)[0].split(" ");
      features = Integer.parseInt(trainInfo[1]);
      readDatasetFromFileToList(TMP_TRAIN_PATH, trainingSetList);

      String[] kek = Files.lines(new File(TMP_TRAIN_PATH).toPath())
              .skip(trainingSetList.size() + 1)
              .limit(1)
              .toArray(String[]::new);
      isTrain = Arrays.stream(kek[0].split(",")).map(Integer::parseInt).map(x -> x == 1).toArray(Boolean[]::new);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Mx startMx() {
    return new VecBasedMx(allTopics.length, features);
  }

  private double accuracy(Mx newWeights) {
    Mx probs = MxTools.multiply(new SparseMx(testSetList.stream()
            .map(s -> (SparseVec) s.getFeatures())
            .toArray(SparseVec[]::new)), MxTools.transpose(newWeights));
    AtomicInteger truePositives = new AtomicInteger(0);
    IntStream.range(0, testSetList.size()).parallel().forEach(i -> {
      final int argmax = VecTools.argmax(probs.row(i));
      if (allTopics[argmax].equals(testSetList.get(i).getLabel())) {
        truePositives.incrementAndGet();
      }
    });
    final double accuracy = truePositives.get() / (double) testSize;
    LOGGER.info("Accuracy {}", accuracy);
    return accuracy;
  }

  private double accuracySKLearn(double alpha) {
    final String pythonCommand = String.format(
            Locale.US,
            "sklearn_one_vs_rest_multiclass.py %f",
            alpha
    );
    final double accuracy = Double.parseDouble(callPython(pythonCommand));
    LOGGER.info("SKLearn accuracy: {}", accuracy);
    return accuracy;
  }

  private double accuracySKLearn() {
    return accuracySKLearn(0.0000009);
  }

  public void testSKLearnParams() {
    splitDatsetOrderedComplete(42);
    readTestTrain();

    LOGGER.info("Updating weights");

    accuracySKLearn(0.000004); // 0.000003, 0.6756
  }

  public void testCompareMultinomialAndSKLearnWindow() {
    Random random = new Random(42);

    double ourAcc = 0;
    double skAcc = 0;

    for (int i = 0; i < 10; i++) {
      final int seed = random.nextInt(Integer.MAX_VALUE);
      splitDatsetWindow(seed);
      readStreaming();

      LOGGER.info("Updating weights");

      long time = System.currentTimeMillis();
      ourAcc += streamingAccuracy(optimizer);
      time = System.currentTimeMillis() - time;
      LOGGER.info("Execution time {}", time);

      splitDatsetOrderedComplete(seed);
      readTestTrain();
      skAcc += accuracySKLearn();
    }
    LOGGER.info("Average accuracy {}", ourAcc / 10);
    LOGGER.info("Average SKLearn accuracy {}", skAcc / 10);
  }

  public void testCompareMultinomialAndSKLearnRandomComplete() {
    Random random = new Random(42);

    double ourAcc = 0;
    double skAcc = 0;

    for (int i = 0; i < 10; i++) {
      splitDatsetRandomComplete(random.nextInt(Integer.MAX_VALUE));
      readTestTrain();

      LOGGER.info("Updating weights");

      long time = System.currentTimeMillis();
      Mx newWeights = optimizeWeights(trainingSetList, startMx());
      time = System.currentTimeMillis() - time;
      LOGGER.info("Execution time {}", time);

      ourAcc += accuracy(newWeights);
      skAcc += accuracySKLearn(0.0000045);
    }
    LOGGER.info("Average accuracy {}", ourAcc / 10);
    LOGGER.info("Average SKLearn accuracy {}", skAcc / 10);
  }

  public void testCompareMultinomialAndSKLearnOrderedComplete() {
    Random random = new Random(42);

    double ourAcc = 0;
    double skAcc = 0;

    for (int i = 0; i < 10; i++) {
      splitDatsetOrderedComplete(random.nextInt(Integer.MAX_VALUE));
      readTestTrain();

      LOGGER.info("Updating weights");

      long time = System.currentTimeMillis();
      Mx newWeights = optimizeWeights(trainingSetList, startMx());
      time = System.currentTimeMillis() - time;
      LOGGER.info("Execution time {}", time);

      ourAcc += accuracy(newWeights);
      skAcc += accuracySKLearn(0.0000045);
    }
    LOGGER.info("Average accuracy {}", ourAcc / 10);
    LOGGER.info("Average SKLearn accuracy {}", skAcc / 10);
  }

  public void testFTRLProximalMultinomial() {

    long splitTime = System.currentTimeMillis();
    splitDatsetOrderedComplete(42); // 5 0.761 0.6936
    splitTime = System.currentTimeMillis() - splitTime;
    LOGGER.info("Split time: {}", splitTime);
    readTestTrain();

    LOGGER.info("Updating weights");

    long time = System.currentTimeMillis();
    Mx newWeights = optimizeWeights(trainingSetList, startMx());
    time = System.currentTimeMillis() - time;
    LOGGER.info("Execution time {}", time);

    accuracy(newWeights);
    //accuracySKLearn();
  }

  @Test
  public void testSelectWindowSizeAndLambda() {
    List<Integer> windowSizes = IntStream.range(4, 21).filter(x -> x % 2 == 0).boxed().collect(Collectors.toList());
    List<Double> lams = Arrays.asList(
            0.05,
            0.1,
            0.15,
            0.2,
            0.25,
            0.3,
            0.35,
            0.4,
            0.45,
            0.5,
            0.55,
            0.6,
            0.65,
            0.7,
            0.75,
            0.8,
            0.85,
            0.9,
            0.95,
            0.99
    );
    double bestAcc = 0;
    int bestSz = 0;
    double bestLam = 0;
    for (Integer sz : windowSizes) {
      for (double lam : lams) {
        LOGGER.info("Started split");
        splitDatsetWindow(42, sz, lam);
        readStreaming();
        LOGGER.info("Streaming read");
        double acc = streamingAccuracy(optimizer);
        if (acc > bestAcc) {
          bestSz = sz;
          bestLam = lam;
          bestAcc = acc;
        }
        LOGGER.info("Best size: {}, best damping factor: {}, best accuracy: {}", bestSz, bestLam, bestAcc);
      }
    }
  }

  public void testSelectSKLearnAlpha() {
    List<Double> alphas = Arrays.asList(
            0.0000008,
            0.00000085,
            0.00000095,
            0.0000007,
            0.00000075,
            0.0000009
    );
    double optAlpha = 0;
    double maxAcc = 0;
    splitDatsetOrderedComplete(42);
    readTestTrain();
    for (double alpha : alphas) {
      double acc = accuracySKLearn(alpha);
      if (acc > maxAcc) {
        optAlpha = alpha;
        maxAcc = acc;
      }
      LOGGER.info("Best alpha: {}", optAlpha);
    }
  }

  public void testCompleteOrdered() {
    splitDatsetOrderedComplete(42);
    readTestTrain();

    Mx newWeights = optimizeWeights(trainingSetList, startMx());
    accuracy(newWeights);
  }

  @Test
  public void testChooseRegularisationParams() {
    //splitDatsetWindow(42);
    readStreaming();
    for (int i = 50; i <= 500; i++) {
      final double alpha = i * 0.1;
      final FTRLProximal warmUpOptimizer = FTRLProximal.builder()
              .alpha(alpha)
              .beta(0.138)
              .lambda1(0.0067)
              .lambda2(0.125)
              .build(allTopics);
      streamingAccuracy(warmUpOptimizer, optimizer);
    }
  }

  private double streamingAccuracy(FTRLProximal optimizer) {
    return streamingAccuracy(optimizer, optimizer);
  }

  private double streamingAccuracy(FTRLProximal warmUpOptimizer, FTRLProximal optimizer) {
    ModelState state = new FTRLState(startMx());
    int truePositives = 0;
    int count = 0;

    final int russiaIndex = 143230;
    final int ukraineIndex = 168018;
    final int putinIndex = 135039;
    final List<Double> russiaList = new ArrayList<>();
    final List<Double> ukraineList = new ArrayList<>();
    final List<Double> putinList = new ArrayList<>();
    final int polIndex = Arrays.asList(allTopics).indexOf("политика");

    for (int i = 0; i < offset; i++) {
      state = warmUpOptimizer.step(trainingSetList.get(i), state);
      russiaList.add(state.weights().get(polIndex, russiaIndex));
      putinList.add(state.weights().get(polIndex, putinIndex));
      ukraineList.add(state.weights().get(polIndex, ukraineIndex));
    }
    for (int i = offset; i < trainingSetList.size(); i++) {
      if (isTrain[i]) {
        state = optimizer.step(trainingSetList.get(i), state);
        russiaList.add(state.weights().get(polIndex, russiaIndex));
        putinList.add(state.weights().get(polIndex, putinIndex));
        ukraineList.add(state.weights().get(polIndex, ukraineIndex));
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
        if (allTopics[argmax].equals(trainingSetList.get(i).getLabel())) {
          truePositives++;
        }
        count++;
      }
    }
    double accuracy = truePositives / (double) count;
    LOGGER.info("Accuracy: {}", accuracy);

    try(PrintWriter writer = new PrintWriter("src/test/resources/tmp.txt")) {
      writer.println(russiaList.stream().map(Object::toString).collect(Collectors.joining(",")));
      writer.println(putinList.stream().map(Object::toString).collect(Collectors.joining(",")));
      writer.println(ukraineList.stream().map(Object::toString).collect(Collectors.joining(",")));
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

    return accuracy;
  }

  private double offlineAccuracy(FTRLProximal optimizer) {
    ModelState state = new FTRLState(startMx());
    int truePositives = 0;
    int count = 0;
    for (int i = 0; i < trainingSetList.size(); i++) {
      if (isTrain[i]) {
        state = optimizer.step(trainingSetList.get(i), state);
      }
    }

    for (int i = 0; i < trainingSetList.size(); i++) {
      if (!isTrain[i]) {
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
        if (allTopics[argmax].equals(trainingSetList.get(i).getLabel())) {
          truePositives++;
        }
        count++;
      }
    }
    double accuracy = truePositives / (double) count;
    LOGGER.info("Accuracy: {}", accuracy);
    return accuracy;
  }

  @Test
  public void graphsWords() {
    final int russiaIndex = 143230;
    final int ukraineIndex = 168018;
    final int putinIndex = 135039;

    splitDatsetWindow(42);
    readStreaming();
    ModelState state = new FTRLState(startMx());

    final List<Double> russiaList = new ArrayList<>();
    final List<Double> ukraineList = new ArrayList<>();
    final List<Double> putinList = new ArrayList<>();

    for (DataPoint t: trainingSetList) {
      state = optimizer.step(t, state);
      final int i = Arrays.asList(allTopics).indexOf("политика");
      russiaList.add(state.weights().get(i, russiaIndex));
      putinList.add(state.weights().get(i, putinIndex));
      final int j = Arrays.asList(allTopics).indexOf("украина");
      ukraineList.add(state.weights().get(j, ukraineIndex));
    }

    LOGGER.info("Russia {}", russiaList);
    LOGGER.info("Putin {}", putinList);
    LOGGER.info("Ukraine {}", ukraineList);
  }

  @Test
  public void testStreaming() {
    //splitDatsetWindow(42); // 12 0.8 0.673
    readStreaming();
    long time = System.currentTimeMillis();
    streamingAccuracy(warmUpOptimizer, optimizer);
    LOGGER.info("Execution time: {}", System.currentTimeMillis() - time);
    //offlineAccuracy(optimizer);
  }

  private Mx optimizeWeights(List<DataPoint> trainingSet, Mx prevWeights) {
    ModelState state = new FTRLState(prevWeights);
    for (DataPoint aTrainingSet : trainingSet) {
      state = optimizer.step(aTrainingSet, state);
    }

    final Mx ans = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    for (int i = 0; i < prevWeights.rows(); i++) {
      VecIterator nz = state.weights().row(i).nonZeroes();
      while (nz.advance()) {
        ans.set(i, nz.index(), nz.value());
      }
    }
    return ans;
  }
}
