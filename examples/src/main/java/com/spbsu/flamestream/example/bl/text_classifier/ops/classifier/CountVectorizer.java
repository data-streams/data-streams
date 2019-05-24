package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CountVectorizer implements Vectorizer {
  private TObjectIntMap<String> countVectorizer;
  private final String pathToVocabulary;

  public CountVectorizer(String pathToVocabulary) {
    this.pathToVocabulary = pathToVocabulary;
  }

  public void init() {
    if (countVectorizer != null) {
      return;
    }

    final File countFile = new File(pathToVocabulary);
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

  @Override
  public Vec vectorize(Document document) {
    init();
    final Map<String, Double> tfIdf = document.tfIdf();
    final int[] indices = new int[tfIdf.size()];
    final double[] values = new double[tfIdf.size()];
    { //convert TF-IDF features to sparse vector
      int ind = 0;
      for (String key : tfIdf.keySet()) {
        final int valueIndex = countVectorizer.get(key);
        indices[ind] = valueIndex;
        values[ind] = tfIdf.get(key);
        ind++;
      }
    }

    return new SparseVec(dim(), indices, values);
  }

  @Override
  public int dim() {
    return countVectorizer.size();
  }

  public int wordIndex(String word) {
    init();
    return countVectorizer.get(word);
  }
}
