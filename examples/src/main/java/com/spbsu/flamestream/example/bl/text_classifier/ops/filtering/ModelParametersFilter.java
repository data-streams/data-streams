package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.spbsu.flamestream.example.bl.text_classifier.model.ModelParameters;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TrainInput;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class ModelParametersFilter implements Function<List<Object>, Stream<List<Object>>> {
  @Override
  public Stream<List<Object>> apply(List<Object> docContainers) {
    if (docContainers.size() > 2) {
      throw new IllegalStateException("Group size should be <= 2");
    }

    if (docContainers.size() == 1 && !(docContainers.get(0) instanceof TrainInput)) {
      throw new IllegalStateException(String.format("The only element in group should be TrainInput: %s (%s)",
              docContainers.get(0), docContainers.get(0).getClass()));
    }

    if (docContainers.size() == 1 || (docContainers.get(0) instanceof ModelParameters
            && docContainers.get(1) instanceof TrainInput)) {
      return Stream.of(docContainers);
    } else {
      return Stream.empty();
    }
  }
}
