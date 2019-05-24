package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordEntry;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class WordContainerOrderingFilter implements Function<List<WordContainer>, Stream<List<WordContainer>>> {

  @Override
  public Stream<List<WordContainer>> apply(List<WordContainer> wordContainers) {
    if (wordContainers.size() > 2) {
      throw new IllegalStateException("Group size should be <= 2");
    }

    if (wordContainers.size() == 1 && !(wordContainers.get(0) instanceof WordEntry)) {
      throw new IllegalStateException(String.format("The only element in group should be WordEntry: %s (%s)",
              wordContainers.get(0), wordContainers.get(0).getClass()));
    }

    if (wordContainers.size() == 1 || (wordContainers.get(0) instanceof WordCounter
            && wordContainers.get(1) instanceof WordEntry)) {
      return Stream.of(wordContainers);
    } else {
      return Stream.empty();
    }
  }
}
