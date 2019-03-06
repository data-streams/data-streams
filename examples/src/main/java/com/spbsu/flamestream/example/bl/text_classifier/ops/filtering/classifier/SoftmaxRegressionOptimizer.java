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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SoftmaxRegressionOptimizer implements Optimizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SoftmaxRegressionOptimizer.class.getName());
  private final List<String> topicList;

  public SoftmaxRegressionOptimizer(String[] topics) {
    topicList = Arrays.asList(topics);
  }

  private Mx l1Gradient(Mx weights) {
    final Mx gradient = new SparseMx(weights.rows(), weights.columns());
    final MxIterator mxIterator = weights.nonZeroes();
    while (mxIterator.advance()) {
      gradient.set(mxIterator.row(), mxIterator.column(), Math.signum(mxIterator.value()));
    }
    return gradient;
  }

  private Mx l2Gradient(SparseMx weights, SparseMx prevWeights) {
    final Mx gradient = new SparseMx(weights.rows(), weights.columns());
    final MxIterator mxIterator = weights.nonZeroes();
    while (mxIterator.advance()) {
      gradient.set(mxIterator.row(), mxIterator.column(), 2 * mxIterator.value());
    }
    return gradient;
  }

  private Mx computeSoftmaxValues(Mx weights, SparseMx trainingSet) {
    final int classesCount = weights.rows();
    final Vec[] rows = IntStream.range(0, trainingSet.rows()).parallel().mapToObj(trainingSet::row).map(point -> {
      double denom = 0.;
      final Vec probs = new ArrayVec(classesCount);
      for (int j = 0; j < classesCount; j++) {
        double numer = 0;
        final VecIterator pointIt = point.nonZeroes();
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

  private SoftmaxData softmaxGradient(Mx weights, SparseMx trainingSet, int[] correctTopics, Mx gradAll) {
    final Mx probabilities = computeSoftmaxValues(weights, trainingSet);
    final int classesCount = weights.rows();

    VecTools.scale(gradAll, 0);
    IntStream.range(0, trainingSet.rows()).forEach(pointIdx -> {
      final Vec point = trainingSet.row(pointIdx);
      for (int i = 0; i < classesCount; i++) {
        final Vec grad = gradAll.row(i);
        final VecIterator vecIterator = point.nonZeroes();
        final boolean isCorrectClass = correctTopics[pointIdx] == i;
        while (vecIterator.advance()) {
          final double proBab = probabilities.get(pointIdx, i);
          grad.adjust(vecIterator.index(), vecIterator.value() * (isCorrectClass ? 1 - proBab : -proBab));
        }
      }
    });

    final double score = IntStream.range(0, trainingSet.rows())
            .mapToDouble(idx -> Math.log(probabilities.get(idx, correctTopics[idx])))
            .average()
            .orElse(Double.NEGATIVE_INFINITY);
    return new SoftmaxData(Math.exp(score), gradAll);
  }

  public Mx optimizeWeights(SparseMx trainingSet, String[] correctTopics, Mx prevWeights) {
    final double alpha = 0.2;
    final double lambda1 = 0.0;
    //final double lambda2 = 1e-1;
    final double maxIter = 100;
    final int[] indeces = Stream.of(correctTopics).mapToInt(topicList::indexOf).toArray();

    double previousValue = 0;
    Mx weights = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    Mx gradAll = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    for (int iteration = 1; iteration <= maxIter; iteration++) {
      LOGGER.info("Iteration {}", iteration);
      final SoftmaxData data = softmaxGradient(weights, trainingSet, indeces, gradAll);
      //LOGGER.info("Softmax gradient: {}", data.gradients);
      LOGGER.info("Loss : {}", data.value);
      //if (Math.abs(data.value - previousValue) < 1e-3) {
      //  break;
      //}

      int nonZero = 0;
      previousValue = data.value;
      final Mx l1 = l1Gradient(weights);
      //final Mx l2 = l2Gradient(weights, prevWeights);
      for (int i = 0; i < weights.rows(); i++) {
        VecTools.scale(l1.row(i), lambda1);
        //VecTools.scale(l2.row(i), lambda2);
        final Vec grad = VecTools.sum(l1.row(i), data.gradients.row(i));
        //grad = VecTools.sum(grad, data.gradients.row(i));
        VecTools.scale(grad, alpha);

        final VecIterator vecIterator = grad.nonZeroes();
        while (vecIterator.advance()) {
          weights.adjust(i, vecIterator.index(), vecIterator.value());
          nonZero += 1;
        }
      }
      System.out.println("NON_ZERO: " + nonZero);
    }
    return weights;
  }

  private class SoftmaxData {
    private final double value;
    private final Mx gradients;

    SoftmaxData(double value, Mx gradients) {
      this.value = value;
      this.gradients = gradients;
    }
  }
}
