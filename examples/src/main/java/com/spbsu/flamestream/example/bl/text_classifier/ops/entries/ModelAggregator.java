package com.spbsu.flamestream.example.bl.text_classifier.ops.entries;

import com.spbsu.flamestream.example.bl.text_classifier.model.ModelParameters;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TrainInput;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Stream;

public class ModelAggregator implements Function<List<Object>, Stream<ModelParameters>> {
  @Override
  public Stream<ModelParameters> apply(List<Object> elems) {
    if (elems.size() == 1) {
      final TrainInput trainInput = (TrainInput) elems.get(0);
      return Stream.of(initial(trainInput));
    } else {
      final TrainInput trainInput = (TrainInput) elems.get(1);
      final ModelParameters modelParameters = (ModelParameters) elems.get(0);

      return Stream.of(merge(modelParameters, trainInput));
    }
  }

  private static ModelParameters initial(TrainInput trainInput) {
    return new ModelParameters(1);
  }

  private static ModelParameters merge(ModelParameters modelParameters, TrainInput trainInput) {
    return new ModelParameters(modelParameters.version() + 1);
  }
}
