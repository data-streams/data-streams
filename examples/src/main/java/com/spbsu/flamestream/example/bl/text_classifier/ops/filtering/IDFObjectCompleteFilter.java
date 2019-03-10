package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final Map<String, List<WordCounter>> buffer = new HashMap<>();

  public void init() {
    buffer.clear();
  }

  @Override
  public Stream<IdfObject> apply(WordCounter wordCounter) {
    //noinspection unchecked
    final List<WordCounter> result = buffer.compute(wordCounter.document(), (s, counters) -> {
      if (counters == null) {
        final ArrayList<WordCounter> wordCounters = new ArrayList<>();
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
