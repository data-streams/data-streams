package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecIterator;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public class SoftmaxRegressionOptimizer implements Optimizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SoftmaxRegressionOptimizer.class.getName());
  private final List<String> topicList;
  private ExecutorService executor = Executors.newFixedThreadPool(8);

  public SoftmaxRegressionOptimizer(String[] topics) {
    topicList = Arrays.asList(topics);
  }

  private Mx l1Gradient(SparseMx weights) {
    final Mx gradient = new SparseMx(weights.rows(), weights.columns());

    for (int i = 0; i < weights.rows(); i++) {
      for (int j = 0; j < weights.columns(); j++) {
        gradient.set(i, j, Math.signum(weights.get(i, j)));
      }
    }

    return gradient;
  }

  private Mx l2Gradient(SparseMx weights, SparseMx prevWeights) {
    //return VecTools.subtract(VecTools.scale(weights, 2), prevWeights); ???
    Mx gradient = new SparseMx(weights.rows(), weights.columns());

    for (int i = 0; i < weights.rows(); i++) {
      for (int j = 0; j < weights.columns(); j++) {
        gradient.set(i, j, 2 * (weights.get(i, j)));
      }
    }

    return gradient;
  }

  private double[][] computeSoftmaxValues(SparseMx weights, Mx trainingSet) {
    final double[][] probabilities = new double[trainingSet.rows()][weights.rows()];

    CountDownLatch latch = new CountDownLatch(trainingSet.rows());
    for (int i = 0; i < trainingSet.rows(); i++) {
      final int finalI = i;
      executor.execute(() -> {
        final Vec x = VecTools.copySparse(trainingSet.row(finalI));
        final Vec mul = MxTools.multiply(weights, x);
        VecTools.exp(mul);
        double denom = VecTools.sum(mul);
        for (int j = 0; j < weights.rows(); j++) {
          final double numer = mul.get(j);
          probabilities[finalI][j] = numer / denom;
        }

        latch.countDown();
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return probabilities;
  }

  private SoftmaxData softmaxGradient(SparseMx weights, Mx trainingSet, int[] correctTopics) {
    final SparseVec[] gradients = new SparseVec[weights.rows()];
    final double[][] probabilities = computeSoftmaxValues(weights, trainingSet);

    SparseVec softmaxValues = new SparseVec(trainingSet.rows());
    for (int i = 0; i < trainingSet.rows(); i++) {
      double val = probabilities[i][correctTopics[i]];
      softmaxValues.set(i, val);
      probabilities[i][correctTopics[i]] = val - 1;
    }
    //LOGGER.info("Softmax values {}", softmaxValues);

    //SparseMx transposedTrainingSet = (SparseMx) MxTools.transpose(trainingSet);

    CountDownLatch latch = new CountDownLatch(weights.rows());
    for (int j = 0; j < weights.rows(); j++) {
      //LOGGER.info("weights {} component", j);

      final int finalJ = j;

      executor.execute(() -> {

        SparseVec grad = new SparseVec(weights.columns());
        for (int i = 0; i < trainingSet.rows(); i++) {
          final SparseVec x = VecTools.copySparse(trainingSet.row(i));
          VecTools.scale(x, probabilities[i][finalJ]);
          VecTools.append(grad, x);
        }

        gradients[finalJ] = grad;

        latch.countDown();
        //LOGGER.info("Finished {}", finalJ);
      });
    }

    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }


    VecTools.log(softmaxValues);
    VecTools.scale(softmaxValues, -1);

    return new SoftmaxData(VecTools.sum(softmaxValues), new SparseMx(gradients));
  }

  public SparseMx optimizeWeights(Mx trainingSet, String[] correctTopics, SparseMx prevWeights) {
    final double alpha = 1e-1;
    final double lambda1 = 1e-2;
    final double lambda2 = 1e-1;
    final double maxIter = 10;
    final int[] indeces = Stream.of(correctTopics).mapToInt(topicList::indexOf).toArray();

    double previousValue = 0;
    SparseMx weights = new SparseMx(prevWeights.rows(), prevWeights.columns());
    for (int iteration = 1; iteration <= maxIter; iteration++) {
      LOGGER.info("Iteration {}", iteration);
      final SoftmaxData data = softmaxGradient(weights, trainingSet, indeces);
      //LOGGER.info("Softmax gradient: {}", data.gradients);
      LOGGER.info("Softmax value : {}", data.value);
      if (Math.abs(data.value - previousValue) < 1e-3) {
        break;
      }

      previousValue = data.value;
      final Mx l1 = l1Gradient(weights);
      final Mx l2 = l2Gradient(weights, prevWeights);
      for (int i = 0; i < weights.rows(); i++) {
        VecTools.scale(l1.row(i), lambda1);
        VecTools.scale(l2.row(i), lambda2);
        Vec grad = VecTools.sum(l1.row(i), l2.row(i));
        grad = VecTools.sum(grad, data.gradients.row(i));
        VecTools.scale(grad, alpha);

        final VecIterator vecIterator = grad.nonZeroes();
        while (vecIterator.advance()) {
          final double prev = weights.get(i, vecIterator.index());
          weights.set(i, vecIterator.index(), prev - vecIterator.value());
        }
      }
    }
    return weights;
  }

  private class SoftmaxData {
    private final double value;
    private final SparseMx gradients;

    SoftmaxData(double value, SparseMx gradients) {
      this.value = value;
      this.gradients = gradients;
    }
  }

}
