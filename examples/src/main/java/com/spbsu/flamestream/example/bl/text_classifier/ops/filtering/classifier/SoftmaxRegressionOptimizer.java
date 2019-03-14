package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxIterator;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecIterator;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.RowsVecArrayMx;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.ArrayVec;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.expleague.commons.util.ArrayTools;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SoftmaxRegressionOptimizer implements Optimizer {
  public static class Builder {
    private double startAlpha = 1;
    private double step = 0.999997;
    private double lambda1 = 0.1;
    private double lambda2 = 0.001;
    private int maxIter = 50;
    private int batchSize = 500;

    public Builder startAlpha(double startAlpha) {
      this.startAlpha = startAlpha;
      return this;
    }

    public Builder step(double step) {
      this.step = step;
      return this;
    }

    public Builder lambda1(double lambda1) {
      this.lambda1 = lambda1;
      return this;
    }

    public Builder lambda2(double lambda2) {
      this.lambda2 = lambda2;
      return this;
    }

    public Builder maxIter(int maxIter) {
      this.maxIter = maxIter;
      return this;
    }

    public Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      return this;
    }

    public SoftmaxRegressionOptimizer build(String[] topics) {
      return new SoftmaxRegressionOptimizer(topics, startAlpha, step, lambda1, lambda2, maxIter, batchSize);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SoftmaxRegressionOptimizer.class.getName());
  private final List<String> topicList;

  final double startAlpha;
  final double step;
  final double lambda1;
  final double lambda2;
  final int maxIter;
  final int batchSize;

  private SoftmaxRegressionOptimizer(String[] topics, double startAlpha, double step, double lambda1, double lambda2, int maxIter, int batchSize) {
    topicList = Arrays.asList(topics);
    this.startAlpha = startAlpha;
    this.step = step;
    this.lambda1 = lambda1;
    this.lambda2 = lambda2;
    this.maxIter = maxIter;
    this.batchSize = batchSize;
  }

  public static Builder builder() {
    return new Builder();
  }

  private Mx l2Gradient(Mx weights, Mx prevWeights) {
    final Mx gradient = new SparseMx(weights.rows(), weights.columns());
    final MxIterator mxIterator = weights.nonZeroes();
    while (mxIterator.advance()) {
      gradient.set(mxIterator.row(), mxIterator.column(), 2 * mxIterator.value());
    }
    final MxIterator prevMxIterator = prevWeights.nonZeroes();
    while (prevMxIterator.advance()) {
      gradient.adjust(prevMxIterator.row(), prevMxIterator.column(), - 2 * prevMxIterator.value());
    }
    return gradient;
  }

  private Mx computeSoftmaxValues(Mx weights, Mx trainingSet) {
    final int classesCount = weights.rows();
    final Vec[] rows = IntStream.range(0, trainingSet.rows()).parallel().mapToObj(trainingSet::row).map(point -> {
      double denom = 0.;
      final Vec probs = new ArrayVec(classesCount);
      for (int j = 0; j < classesCount; j++) {
        double numer = 0;
        final VecIterator pointIt = point.nonZeroes();
        final Vec weightsRow = weights.row(j);
        while (pointIt.advance()) {
          numer += pointIt.value() * weightsRow.get(pointIt.index());
        }
        denom += Math.exp(numer);
        probs.set(j, Math.exp(numer));
      }

      for (int i = 0; i < probs.dim(); i++) {
        final double value = probs.get(i);
        probs.set(i, value / denom);
      }
      return probs;
    }).toArray(Vec[]::new);

    return new RowsVecArrayMx(rows);
  }

  private Mx computeStochasticSoftmaxValues(TIntList indices, Mx weights, Mx trainingSet) {
    final int classesCount = weights.rows();
    final Vec[] rows = IntStream.range(0, batchSize).map(indices::get).parallel().mapToObj(trainingSet::row).map(point -> {
      double denom = 0.;
      final Vec probs = new ArrayVec(classesCount);
      for (int j = 0; j < classesCount; j++) {
        double numer = 0;
        final VecIterator pointIt = point.nonZeroes();
        //final Vec weightsRow = weights.row(j);
        while (pointIt.advance()) {
          numer += pointIt.value() * weights.get(j, pointIt.index());
        }
        denom += Math.exp(numer);
        probs.set(j, Math.exp(numer));
      }

      for (int i = 0; i < probs.dim(); i++) {
        final double value = probs.get(i);
        probs.set(i, value / denom);
      }
      return probs;
    }).toArray(Vec[]::new);

    return new RowsVecArrayMx(rows);
  }

  private SoftmaxData softmaxStochasticGradient(final TIntList indices, final Mx weights, final Mx trainingSet, final int[] correctTopics, Mx gradAll) {
    final Mx probabilities = computeStochasticSoftmaxValues(indices, weights, trainingSet);
    final int classesCount = weights.rows();

    VecTools.scale(gradAll, 0);
    IntStream.range(0, classesCount).parallel().forEach(i -> {
      for (int index = 0; index < batchSize; index++) {
        int j = indices.get(index);
        final Vec point = trainingSet.row(j);
        final Vec grad = gradAll.row(i);
        final VecIterator vecIterator = point.nonZeroes();
        final boolean isCorrectClass = correctTopics[j] == i;
        final double proBab = probabilities.get(index, i);
        final double denom = isCorrectClass ? 1 - proBab : -proBab;
        while (vecIterator.advance()) {
          grad.adjust(vecIterator.index(), vecIterator.value() * denom);
        }
      }
    });

    VecTools.scale(gradAll, trainingSet.rows() / (double) batchSize);

    final double score = IntStream.range(0, batchSize)
            .mapToDouble(idx -> Math.log(probabilities.get(idx, correctTopics[indices.get(idx)])))
            .average()
            .orElse(Double.NEGATIVE_INFINITY);
    return new SoftmaxData(Math.exp(score), gradAll);
  }

  public Mx optimizeWeights(Mx trainingSet, String[] correctTopics, Mx prevWeights) {
    double alpha = startAlpha;
    /*for (String topic: correctTopics) {
      if (topicList.indexOf(topic) == -1) {
        LOGGER.info("Bad topic {}", topic);
      }
    }*/
    //LOGGER.info("kek{}kek{}kek", topicList.get(0), "соцсети");
    //LOGGER.info("index {}", topicList.indexOf("соцсети"));
    final int[] indices = Stream.of(correctTopics).mapToInt(topicList::indexOf).toArray();

    final Mx weights = VecTools.copy(prevWeights);
    //final Mx weights = new VecBasedMx(prevWeights.rows(), prevWeights.columns());

    final Mx gradAll = new VecBasedMx(prevWeights.rows(), prevWeights.columns());

    final TIntList stochasticIndices = new TIntArrayList(ArrayTools.sequence(0, trainingSet.rows()));
    final Random random = new Random();

    for (int iteration = 1; iteration <= maxIter; iteration++) {
      LOGGER.info("Iteration {}", iteration);
      stochasticIndices.shuffle(random);
      final SoftmaxData data = softmaxStochasticGradient(stochasticIndices, weights, trainingSet, indices, gradAll);
      LOGGER.info("Score : {}", data.score);

      { // l1 regularization: update gradients for non-zero weights
        final MxIterator iterator = weights.nonZeroes();
        while (iterator.advance()) {
          data.gradients.adjust(
                  iterator.row(),
                  iterator.column(),
                  lambda1 * Math.signum(iterator.value())
          );
        }
      }
      { // l1 regularization: update gradients for zero weights
        final MxIterator iterator = data.gradients.nonZeroes();
        while (iterator.advance()) {
          if (Math.abs(weights.get(iterator.row(), iterator.column())) <= 0) {
            final double gradValue = data.gradients.get(iterator.row(), iterator.column());
            if (gradValue < -lambda1) {
              data.gradients.adjust(iterator.row(), iterator.column(), lambda1);
            } else if (gradValue > lambda1) {
              data.gradients.adjust(iterator.row(), iterator.column(), -lambda1);
            } else if (gradValue >= -lambda1 && gradValue <= lambda1) {
              data.gradients.set(iterator.row(), iterator.column(), 0.0);
            }
          }
        }
      }

      final Mx l2 = l2Gradient(weights, prevWeights);
      for (int i = 0; i < weights.rows(); i++) {
        VecTools.scale(l2.row(i), lambda2);
        final Vec grad = VecTools.sum(l2.row(i), data.gradients.row(i));
        VecTools.scale(grad, alpha);

        final VecIterator vecIterator = grad.nonZeroes();
        while (vecIterator.advance()) {
          weights.adjust(i, vecIterator.index(), vecIterator.value());
        }
      }

      alpha *= step;
    }

    int nonZeros = 0;
    final MxIterator iterator = weights.nonZeroes();
    while (iterator.advance()) {
      nonZeros++;
    }
    LOGGER.info("Non-Zeroes: " + nonZeros);

    return weights;
  }

  private class SoftmaxData {
    private final double score;
    private final Mx gradients;

    SoftmaxData(double score, Mx gradients) {
      this.score = score;
      this.gradients = gradients;
    }
  }
}
