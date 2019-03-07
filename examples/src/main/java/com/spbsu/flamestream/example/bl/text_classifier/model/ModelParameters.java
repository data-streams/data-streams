package com.spbsu.flamestream.example.bl.text_classifier.model;


public class ModelParameters {
  int version;

  public ModelParameters(int version) {
      this.version = version;
  }

  public int version() { return this.version; }

  public String toString() {
    return "MP version " + version;
  }
}
