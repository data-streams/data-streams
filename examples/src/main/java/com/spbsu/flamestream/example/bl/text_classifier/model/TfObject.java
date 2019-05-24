package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.TextUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class TfObject implements DocContainer {
  private final Map<String, Integer> counts;
  private final String docName;
  private final String partitioning;
  private final String label;
  private final int number;

  private TfObject(String docName, Stream<String> words, String partitioning, String label, int number) {
    this.docName = docName;
    this.number = number;
    this.partitioning = partitioning;
    this.label = label;
    counts = new HashMap<>();
    words.forEach(s -> counts.merge(s, 1, Integer::sum));
  }

  public static TfObject ofText(TextDocument textDocument) {
    final Stream<String> result = TextUtils.text2words(textDocument.content());
    return new TfObject(
            textDocument.name(),
            result,
            textDocument.partitioning(),
            textDocument.label(),
            textDocument.number()
    );
  }

  public String label() {
    return label;
  }

  public Map<String, Integer> counts() {
    return counts;
  }

  public int number() {
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
