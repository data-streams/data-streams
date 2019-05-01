package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class TfObject implements DocContainer {
  private final Map<String, Integer> counts;
  private final String docName;
  private final String partitioning;
  private final long number;

  private TfObject(String docName, Stream<String> words, String partitioning, long number) {
    this.docName = docName;
    this.number = number;
    this.partitioning = partitioning;
    counts = new HashMap<>();
    words.forEach(s -> counts.merge(s, 1, Integer::sum));
  }

  public static TfObject ofText(TextDocument textDocument) {
    return new TfObject(
            textDocument.name(),
            SklearnSgdPredictor.text2words(textDocument.content()),
            textDocument.partitioning(),
            textDocument.number()
    );
  }

  public Map<String, Integer> counts() {
    return counts;
  }

  public long number() {
    return number;
  }

  @Override
  public String document() {
    return docName;
  }

  @Override
  public String partitioning() {
    return partitioning;
  }
}
