package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.ModelParameters;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class ClassifyFilterBack implements Function<List<Object>, Stream<Object>> {

  @Override
  public Stream<Object> apply(List<Object> docContainers) {
    if (docContainers.size() > 2) {
      throw new IllegalStateException("Group size should be <= 2");
    }
    if (docContainers.size() == 0) {
      throw new IllegalStateException("Group size should be > 0");
    }

    if (docContainers.size() == 1) {
      Object elem = docContainers.get(0);
      if (elem instanceof TfIdfObject) {
        return Stream.of(elem);
      } else if (elem instanceof ModelParameters) {
        return Stream.of(elem);
      } else {
        throw new IllegalStateException(String.format("The only element in group is of wrong type: %s (%s)",
                docContainers.get(0), docContainers.get(0).getClass()
        ));
      }
    } else {
      Object first = docContainers.get(0);
      Object second = docContainers.get(1);
      if (first instanceof ModelParameters && second instanceof TfIdfObject) {
        return Stream.of(first);
      } else {
        throw new IllegalStateException(String.format("Something wrong with types: %s (%s) %s (%s)",
                docContainers.get(0), docContainers.get(0).getClass(),
                docContainers.get(1), docContainers.get(1).getClass()
        ));
      }
    }
  }
}
