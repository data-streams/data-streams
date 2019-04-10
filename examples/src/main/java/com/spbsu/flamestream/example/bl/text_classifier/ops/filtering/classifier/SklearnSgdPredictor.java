package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecIterator;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.RowsVecArrayMx;
import com.expleague.commons.math.vectors.impl.vectors.ArrayVec;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.google.common.annotations.VisibleForTesting;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SklearnSgdPredictor implements TopicsPredictor {
  private static final Pattern PATTERN = Pattern.compile("\\b\\w\\w+\\b", Pattern.UNICODE_CHARACTER_CLASS);

  private final String weightsPath;
  private final String cntVectorizerPath;

  //lazy loading
  private TObjectIntMap<String> countVectorizer;
  private Vec intercept;
  private Mx weights;
  private String[] topics;

  public SklearnSgdPredictor(String cntVectorizerPath, String weightsPath) {
    this.weightsPath = weightsPath;
    this.cntVectorizerPath = cntVectorizerPath;
  }

  private SparseVec vectorize(Map<String, Double> tfIdf) {
    final int[] indices = new int[tfIdf.size()];
    final double[] values = new double[tfIdf.size()];

    int ind = 0;
    for (String key : tfIdf.keySet()) {
      final int valueIndex = countVectorizer.get(key);
      indices[ind] = valueIndex;
      values[ind] = tfIdf.get(key);
      ind++;
    }

    return new SparseVec(countVectorizer.size(), indices, values);
  }

  @Override
  public Topic[] predict(Document document) {
    loadMeta();
    loadVocabulary();

    final Vec probabilities;
    { // compute topic probabilities
      final SparseVec vectorized = vectorize(document.tfIdf());
      final VecIterator docNZIt = vectorized.nonZeroes();
      Vec score =  MxTools.multiply(weights, vectorized);
      final Vec sum = VecTools.sum(score, intercept);
      final Vec scaled = VecTools.scale(sum, -1);
      VecTools.exp(scaled);

      final Vec vecOnes = new ArrayVec(score.dim());
      VecTools.fill(vecOnes, 1);

      probabilities = VecTools.sum(scaled, vecOnes);
      for (int i = 0; i < probabilities.dim(); i++) {
        double changed = 1 / probabilities.get(i);
        probabilities.set(i, changed);
      }
      final double rowSum = VecTools.sum(probabilities);
      VecTools.scale(probabilities, 1 / rowSum);
    }

    final Topic[] result = new Topic[probabilities.dim()];
    { //fill in topics
      for (int index = 0; index < probabilities.dim(); index++) {
        result[index] = new Topic(topics[index], Integer.toString(index), probabilities.get(index));
      }
    }
    return result;
  }

  @Override
  public void updateWeights(Mx weights) {
    this.weights = weights;
  }

  public Mx getWeights() {
    return weights;
  }

  public String[] getTopics() {
    return topics;
  }

  public void init() {
    loadMeta();
    loadVocabulary();
  }

  private void loadMeta() {
    if (weights != null) {
      return;
    }

    final File metaData = new File(weightsPath);
    try (final BufferedReader br = new BufferedReader(new InputStreamReader(
            new FileInputStream(metaData),
            StandardCharsets.UTF_8
    ))) {
      final double[] meta = parseDoubles(br.readLine());
      final int classes = (int) meta[0];
      final int currentFeatures = (int) meta[1];
      topics = new String[classes];
      for (int i = 0; i < classes; i++) {
        topics[i] = br.readLine();
      }

      final SparseVec[] coef = new SparseVec[classes];
      String line;
      for (int index = 0; index < classes; index++) {
        line = br.readLine();
        String[] rawSplit = line.split(" ");

        int[] indeces = new int[rawSplit.length / 2];
        double[] values = new double[rawSplit.length / 2];
        for (int i = 0; i < rawSplit.length; i += 2) {
          int valueIndex = Integer.parseInt(rawSplit[i]);
          double value = Double.parseDouble(rawSplit[i + 1]);

          indeces[i / 2] = valueIndex;
          values[i / 2] = value;
        }

        coef[index] = new SparseVec(currentFeatures, indeces, values);
      }

      weights = new RowsVecArrayMx(coef);
      MxTools.transpose(weights);

      line = br.readLine();
      final double[] parsedIntercept = parseDoubles(line);
      intercept = new ArrayVec(parsedIntercept, 0, parsedIntercept.length);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void loadVocabulary() {
    if (countVectorizer != null) {
      return;
    }

    final File countFile = new File(cntVectorizerPath);
    countVectorizer = new TObjectIntHashMap<>();
    try (final BufferedReader countFileReader = new BufferedReader(new InputStreamReader(
            new FileInputStream(countFile),
            StandardCharsets.UTF_8
    ))) {
      String line;
      while ((line = countFileReader.readLine()) != null) {
        final String[] items = line.split(" ");
        final String key = items[0];
        final int value = Integer.parseInt(items[1]);
        countVectorizer.put(key, value);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public int wordIndex(String word) {
    loadVocabulary();
    return countVectorizer.get(word);
  }

  public static Stream<String> text2words(String text) {
    final Matcher matcher = PATTERN.matcher(text);
    final Iterable<String> iterable = () -> new Iterator<String>() {
      @Override
      public boolean hasNext() {
        return matcher.find();
      }

      @Override
      public String next() {
        return matcher.group(0);
      }
    };
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  private static double[] parseDoubles(String line) {
    return Arrays
            .stream(line.split(" "))
            .mapToDouble(Double::parseDouble)
            .toArray();
  }
}
