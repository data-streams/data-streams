package com.spbsu.benchmark.flink.lenta;

import com.google.common.collect.Iterators;
import com.spbsu.flamestream.example.bl.text_classifier.LentaCsvTextDocumentsReader;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.shaded.netty4.io.netty.util.collection.IntObjectHashMap;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;

public class FlinkBenchTest {
  static private long blinkAtMillis;

  static class ExamplesTopicPredictor implements FlinkBench.SerializableTopicsPredictor {
    public static final SklearnSgdPredictor predictor = new SklearnSgdPredictor(
            "../../examples/src/main/resources/cnt_vectorizer",
            "../../examples/src/main/resources/classifier_weights"
    );

    static {
      predictor.init();
    }

    @Override
    public Topic[] predict(Document document) {
      return predictor.predict(document);
    }
  }

  static class TextDocumentIterator implements Iterator<TextDocument>, Serializable {
    private FileInputStream inputStream;
    private Iterator<TextDocument> iterator;

    @Override
    public boolean hasNext() {
      return iterator().hasNext();
    }

    @Override
    public TextDocument next() {
      return iterator().next();
    }

    private Iterator<TextDocument> iterator() {
      if (iterator != null) {
        return iterator;
      }
      try {
        if (inputStream == null) {
          inputStream = new FileInputStream("../../examples/src/main/resources/lenta/lenta-ru-news.csv");
        }
        return iterator = LentaCsvTextDocumentsReader.documents(inputStream).iterator();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finalize() throws Throwable {
      if (inputStream != null) {
        inputStream.close();
      }
    }
  }

  @Test
  public void testPredictionDataStream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(2000);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 100));
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
    final long blinkPeriodMillis = 7000;
    blinkAtMillis = System.currentTimeMillis() + blinkPeriodMillis;
    CollectSink.values.clear();
    predictionDataStream(env.fromCollection(new TextDocumentIterator(), TextDocument.class).setParallelism(1))
            .map(value -> {
              if (System.currentTimeMillis() > blinkAtMillis) {
                blinkAtMillis = System.currentTimeMillis() + blinkPeriodMillis;
                throw new RuntimeException("blink");
              }
              return value;
            })
            .addSink(new CollectSink()).setParallelism(1);
    env.execute();
    assertEquals(CollectSink.values.size(), Iterators.size(new TextDocumentIterator()));
    //try (final FileWriter out = new FileWriter("predictions.csv")) {
    //  dumpToCsv(new ArrayList<>(CollectSink.values.values()), out);
    //}
  }

  private DataStream<Prediction> predictionDataStream(DataStreamSource<TextDocument> source) {
    return FlinkBench.predictionDataStream(new ExamplesTopicPredictor(), source);
  }

  private void dumpToCsv(List<Prediction> predictions, Appendable out) throws IOException {
    final CSVPrinter csvPrinter = new CSVPrinter(out, CSVFormat.DEFAULT);
    final String[] topicNames = predictions.isEmpty() ? new String[]{} :
            Stream.of(predictions.get(0).topics()).map(Topic::name).toArray(String[]::new);
    csvPrinter.print("document");
    for (final String topicName : topicNames) {
      csvPrinter.print(topicName);
    }
    csvPrinter.println();
    for (final Prediction prediction : predictions) {
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
    }
  }

  // create a testing sink
  private static class CollectSink implements SinkFunction<Prediction> {
    // must be static
    public static final IntObjectHashMap<Prediction> values = new IntObjectHashMap<>();

    @Override
    public synchronized void invoke(Prediction value) throws Exception {
      values.put(value.tfIdf().number(), value);
    }
  }
}
