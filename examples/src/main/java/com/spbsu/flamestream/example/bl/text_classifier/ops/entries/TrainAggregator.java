package com.spbsu.flamestream.example.bl.text_classifier.ops.entries;

import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Stream;

public class TrainAggregator implements Function<List<Object>, Stream<List<TfIdfObject>>> {
  @Override
  public Stream<List<TfIdfObject>> apply(List<Object> elems) {
    if (elems.size() == 1) {
      List<TfIdfObject> l = new CopyOnWriteArrayList<>();
      l.add((TfIdfObject) elems.get(0));
      return Stream.of(l);
    } else {
      final List lst = (List) elems.get(0);
      final TfIdfObject tfIdfObject = (TfIdfObject) elems.get(1);

      lst.add(tfIdfObject);
      return Stream.of(lst);
    }
  }
}
