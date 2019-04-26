package com.spbsu.benchmark.flink.lenta;

import com.spbsu.flamestream.example.bl.text_classifier.LentaCsvTextDocumentsReader;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FlinkBenchTest {
  @Test
  public void testPredictionDataStream() throws Exception {
    StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    environment.setParallelism(1);
    CollectSink.values.clear();
    final List<TextDocument> documents;
    try (final FileInputStream inputStream = new FileInputStream(
            "../../examples/src/main/resources/lenta/lenta-ru-news.csv"
    )) {
      documents = LentaCsvTextDocumentsReader.documents(inputStream).collect(Collectors.toList());
    }
    FlinkBench.predictionDataStream(
            "../../examples/src/main/resources/cnt_vectorizer",
            "../../examples/src/main/resources/classifier_weights",
            environment.fromCollection(documents)
    ).addSink(new CollectSink());
    environment.execute();
  }

  // create a testing sink
  private static class CollectSink implements SinkFunction<Prediction> {
    // must be static
    public static final List<Prediction> values = new ArrayList<>();

    @Override
    public synchronized void invoke(Prediction value, Context context) {
      values.add(value);
    }
  }
}
