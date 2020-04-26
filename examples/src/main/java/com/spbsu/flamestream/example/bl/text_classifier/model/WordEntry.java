
package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.WordContainer;

public class WordEntry implements WordContainer, DocContainer {
  private final String word;
  private final String docId;
  private final String partitioning;
  private final boolean labeled;
  private final int idfCardinality;

  public WordEntry(String word, String docId, int idfCardinality, String partitioning, boolean labeled) {
    this.word = word;
    this.docId = docId;
    this.idfCardinality = idfCardinality;
    this.partitioning = partitioning;
    this.labeled = labeled;
  }

  @Override
  public String word() {
    return word;
  }

  @Override
  public String document() {
    return docId;
  }

  @Override
  public String partitioning() {
    return partitioning;
  }

  @Override
  public boolean labeled() {
    return labeled;
  }

  public int idfCardinality() {
    return idfCardinality;
  }

  @Override
  public String toString() {
    return String.format("<WE> doc hash: %d,  word: >%s<, doc: >%s< idf cardinality %d",
            document().hashCode(), word, docId, idfCardinality
    );
  }
}
