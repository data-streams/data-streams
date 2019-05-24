package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class TfIdfFilter implements Function<List<DocContainer>, Stream<DocContainer>> {

  @Override
  public Stream<DocContainer> apply(List<DocContainer> elements) {
    if (elements.size() < 2 || !elements.get(0).document().equals(elements.get(1).document())) {
      return Stream.of();
    } else {
      final IdfObject idf;
      final TfObject tf;
      if (elements.get(0) instanceof TfObject && elements.get(1) instanceof IdfObject) {
        idf = (IdfObject) elements.get(1);
        tf = (TfObject) elements.get(0);
      } else {
        idf = (IdfObject) elements.get(0);
        tf = (TfObject) elements.get(1);
      }
      final TfIdfObject res = new TfIdfObject(tf, idf);
      return Stream.of(res);
    }
  }
}
