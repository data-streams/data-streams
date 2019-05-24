package com.spbsu.flamestream.example.bl.text_classifier.ops;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;

public class DocContainerOrderingFilter implements Function<List<DocContainer>, Stream<List<DocContainer>>> {

  @Override
  public Stream<List<DocContainer>> apply(List<DocContainer> docContainers) {
    if (docContainers.size() > 2) {
      throw new IllegalStateException("Group size should be <= 2");
    }

    if (docContainers.size() == 1 && !(docContainers.get(0) instanceof WordCounter)) {
      throw new IllegalStateException(String.format("The only element in group should be WordCounter: %s (%s)",
              docContainers.get(0), docContainers.get(0).getClass()));
    }

    if (docContainers.size() == 1 || (docContainers.get(0) instanceof IdfObject
            && docContainers.get(1) instanceof WordCounter)) {
      if (docContainers.size() == 2 && !docContainers.get(0).document().equals(docContainers.get(1).document())) {
        return Stream.of(Collections.singletonList(docContainers.get(1)));
      }
      return Stream.of(docContainers);
    } else {
      return Stream.empty();
    }
  }
}
