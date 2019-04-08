package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.MathTools;
import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecIterator;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import org.apache.commons.lang.math.IntRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FTRLProximalOptimizer implements Optimizer, BiClassifierOptimizer {

  public static class Builder {
    private double alpha = 0.2;
    private double beta = 0.1;
    private double lambda1 = 0.1;
    private double lambda2 = 0.001;

    public Builder alpha(double alpha) {
      this.alpha = alpha;
      return this;
    }

    public Builder beta(double beta) {
      this.beta = beta;
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

    public FTRLProximalOptimizer build(String[] topics) {
      return new FTRLProximalOptimizer(alpha, beta, lambda1, lambda2, topics);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FTRLProximalOptimizer.class.getName());

  private final double alpha;
  private final double beta;
  private final double lambda1;
  private final double lambda2;
  private final List<String> topicList;

  private FTRLProximalOptimizer(double alpha, double beta, double lambda1, double lambda2, String[] topicList) {
    this.alpha = alpha;
    this.beta = beta;
    this.lambda1 = lambda1;
    this.lambda2 = lambda2;
    this.topicList = Arrays.asList(topicList);
  }

  @Override
  public Vec optimizeWeights(Mx trainingSet, int[] isCorrect, Vec prevWeights) {
    SparseVec zed = new SparseVec(prevWeights.dim());
    SparseVec norm = new SparseVec(prevWeights.dim());
    Vec x;
    SparseVec w = new SparseVec(prevWeights.dim());
    for (int j = 0; j < 30; j++) {
      for (int t = 0; t < trainingSet.rows(); t++) {
        x = trainingSet.row(t);
        VecIterator iterator = x.nonZeroes();
        while (iterator.advance()) {
          double z = zed.get(iterator.index());
          if (Math.abs(z) > lambda1) {
            double val = -(z - Math.signum(z) * lambda1) /
                    ((beta + Math.sqrt(norm.get(iterator.index()))) / alpha + lambda2);
            w.set(iterator.index(), val);
          }
        }
        double p = MathTools.sigmoid(VecTools.multiply(x, w));
        iterator = x.nonZeroes();
        while (iterator.advance()) {
          double g = (p - isCorrect[t]) * iterator.value();
          double sigma = (Math.sqrt(norm.get(iterator.index()) + g * g) - Math.sqrt(norm.get(iterator.index()))) / alpha;
          zed.set(iterator.index(), zed.get(iterator.index()) + g - sigma * w.get(iterator.index()));
          norm.set(iterator.index(), norm.get(iterator.index()) + g * g);
        }
      }
    }
    return w;
  }

  @Override
  public Mx optimizeWeights(Mx trainingSet, String[] correctTopics, Mx prevWeights) {
    final int[] indices = Stream.of(correctTopics).mapToInt(topicList::indexOf).toArray();
    Mx weights = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    Mx zed = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    Mx norm = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    for (int it = 0; it < 3; it++) {
      double score = 0;
      for (int i = 0; i < trainingSet.rows(); i++) {
        final int finalI = i;
        Vec x = trainingSet.row(i);
        IntStream.range(0, weights.rows()).parallel().forEach(j -> {
          VecIterator iterator = x.nonZeroes();
          while (iterator.advance()) {
            double z = zed.get(j, iterator.index());
            if (Math.abs(z) > lambda1) {
              double val = -(z - Math.signum(z) * lambda1) /
                      ((beta + Math.sqrt(norm.get(j, iterator.index()))) / alpha + lambda2);
              weights.set(j, iterator.index(), val);
            } else {
              weights.set(j, iterator.index(), 0);
            }
          }
        });
        Vec p = MxTools.multiply(weights, x);
        VecTools.exp(p);
        double denom = VecTools.sum(p);
        VecTools.scale(p, 1 / denom);
        score += p.get(indices[i]);
        //LOGGER.info("Score: {}", score);
        if (i % 100 == 0)
          LOGGER.info("Iteration: {} {}", it, i);
        IntStream.range(0, weights.rows()).parallel().forEach(j -> {
          VecIterator iterator = x.nonZeroes();
          while (iterator.advance()) {
            double g = (indices[finalI] == j ? p.get(j) - 1 : p.get(j)) * iterator.value();
            double sigma =
                    (Math.sqrt(norm.get(j, iterator.index()) + g * g) - Math.sqrt(norm.get(j, iterator.index()))) / alpha;
            zed.set(j, iterator.index(), zed.get(j, iterator.index()) + g - sigma * weights.get(j, iterator.index()));
            norm.set(j, iterator.index(), norm.get(j, iterator.index()) + g * g);
          }
        });
      }
      score /= trainingSet.rows();
      LOGGER.info("Iteration {}, average score {}", it, score);
    }
    return new VecBasedMx(weights);
  }
}
