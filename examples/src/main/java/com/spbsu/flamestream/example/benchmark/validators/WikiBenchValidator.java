package com.spbsu.flamestream.example.benchmark.validators;

import com.spbsu.flamestream.example.benchmark.BenchValidator;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import gnu.trove.set.TLongSet;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Artem
 * Date: 03.01.2018
 */
public abstract class WikiBenchValidator implements BenchValidator<WordIndexAdd> {
  final Map<String, TLongSet> expectedPositions = new ConcurrentHashMap<>();

  @Override
  public void accept(WordIndexAdd wordIndexAdd) {
    expectedPositions.computeIfPresent(wordIndexAdd.word(), (word, set) -> {
      Arrays.stream(wordIndexAdd.positions()).forEach(set::remove);
      return set;
    });
  }

  @Override
  public void stop() {
    expectedPositions.forEach((word, set) -> {
      if (!set.isEmpty()) {
        throw new IllegalStateException("The following positions for word \"" + word + "\" were not received: " + set);
      }
    });
  }
}
