package com.spbsu.flamestream.example.bl.text_classifier.model;

import org.testng.mustache.Model;

public class ClassifyParameters {
  private ModelParameters _modelParameters;
  private TfIdfObject _tfIdfObject;

  public ClassifyParameters(ModelParameters modelParameters, TfIdfObject tfIdfObject) {
    _modelParameters = modelParameters;
    _tfIdfObject = tfIdfObject;
  }

  public TfIdfObject tfIdfObject() {
    return _tfIdfObject;
  }

  public ModelParameters modelParameters() {
    return _modelParameters;
  }
}
