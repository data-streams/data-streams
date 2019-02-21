package com.spbsu.flamestream.example.bl.tfidf.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidf.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidf.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordCounter;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class TrainOrderingFilter implements Function<List<Object>, Stream<List<Object>>> {
  @Override
  public Stream<List<Object>> apply(List<Object> docContainers) {
    if (docContainers.size() > 2) {
      throw new IllegalStateException("Group size should be <= 2");
    }

    System.out.println("FFFFFFF: " + docContainers);

    if (docContainers.size() == 1 && !(docContainers.get(0) instanceof TfIdfObject)) {
      throw new IllegalStateException(String.format("The only element in group should be WordCounter: %s (%s)",
              docContainers.get(0), docContainers.get(0).getClass()));
    }


    if (docContainers.size() == 1 || (docContainers.get(0) instanceof List
            && docContainers.get(1) instanceof TfIdfObject)) {
      System.out.println("FFFFFFF-22222: " + docContainers);

      return Stream.of(docContainers);
    } else {
      return Stream.empty();
    }

    //return Stream.empty();
  }
}
