package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/*
 * This hack works because of the following facts:
 * 1. There are no reorderings up to the stream, hence, tombstones will not arrive
 * 2. All word counters have the same global time, so corresponding row in acker will not be incorrectly nullified
 * 3. The loss of the field do not lead to the loss of exactly once
 * */
public class IDFObjectCompleteFilter implements Function<WordCounter, Stream<IdfObject>> {
  //Do not put state in class fields in a general case
  private final Map<String, Set<WordCounter>> buffer = new HashMap<>();

  public void init() {
    buffer.clear();
  }

  @Override
  public Stream<IdfObject> apply(WordCounter wordCounter) {
    final Set<WordCounter> result = buffer.compute(wordCounter.document(), (s, counters) -> {
      if (counters == null) {
        final Set<WordCounter> wordCounters = new HashSet<>();
        wordCounters.add(wordCounter);
        return wordCounters;
      }
      counters.add(wordCounter);
      return counters;
    });

    if (result.size() == wordCounter.idfCardinality()) {
      final IdfObject idfObject = new IdfObject(result);
      buffer.remove(wordCounter.document());
      return Stream.of(idfObject);
    }
    return Stream.empty();
  }
}
