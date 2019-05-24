package com.spbsu.flamestream.example.benchmark.validators.classifier;

import com.spbsu.flamestream.example.benchmark.BenchValidator;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class Lenta1100Validator implements BenchValidator<Prediction> {
  private final CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(
          new FileOutputStream("/tmp/predictions.csv"),
          StandardCharsets.UTF_8
  ), CSVFormat.DEFAULT);

  public Lenta1100Validator() throws IOException {}

  static private String[] topicNames;

  @Override
  public int inputLimit() {
    return 1095;
  }

  @Override
  public int expectedOutputSize() {
    return 1095;
  }

  @Override
  public void stop() {
    try {
      if (topicNames == null) {
        csvPrinter.println();
      }
      csvPrinter.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void accept(Prediction prediction) {
    try {
      if (topicNames == null) {
        topicNames = Stream.of(prediction.topics()).map(Topic::name).toArray(String[]::new);
        for (final String topicName : topicNames) {
          csvPrinter.print(topicName);
        }
        csvPrinter.println();
      }
      csvPrinter.print(prediction.tfIdf().document());
      if (prediction.topics().length != topicNames.length) {
        throw new RuntimeException("different topics number");
      }
      int i = 0;
      for (final Topic topic : prediction.topics()) {
        if (!topic.name().equals(topicNames[i])) {
          throw new RuntimeException("inconsistent topics order");
        }
        i++;
        csvPrinter.print(topic.probability());
      }
      csvPrinter.println();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
