package com.spbsu.benchmark.flink.lenta;

import com.spbsu.flamestream.example.bl.text_classifier.LentaCsvTextDocumentsReader;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
  void prepareChmLenta() throws IOException {
    try (
            final FileInputStream in =
                    new FileInputStream("../../examples/src/main/resources/lenta/news_lenta.csv");
            final FileOutputStream out =
                    new FileOutputStream("../../examples/src/main/resources/lenta/chm_news_lenta.csv");
            final CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(out), CSVFormat.DEFAULT)
    ) {
      StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              new CSVParser(new BufferedReader(new InputStreamReader(
                      in,
                      StandardCharsets.UTF_8
              )), CSVFormat.DEFAULT.withFirstRecordAsHeader()).iterator(),
              Spliterator.IMMUTABLE
      ), false)
              .filter(csvRecord -> csvRecord.get(4).contains("/2018/06/") || csvRecord.get(4).contains("/2018/07/"))
              .forEach(values -> {
                try {
                  csvPrinter.printRecord(values);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  @Test
  void prepareFewTopics() throws IOException {
    try (
            final FileInputStream in =
                    new FileInputStream("../../examples/src/main/resources/lenta/news_lenta.csv");
            final FileOutputStream out =
                    new FileOutputStream("../../examples/src/main/resources/lenta/few_topics_news_lenta.csv");
            final CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(out), CSVFormat.DEFAULT)
    ) {
      StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              new CSVParser(new BufferedReader(new InputStreamReader(
                      in,
                      StandardCharsets.UTF_8
              )), CSVFormat.DEFAULT.withFirstRecordAsHeader()).iterator(),
              Spliterator.IMMUTABLE
      ), false)
              .filter(csvRecord -> {
                switch (csvRecord.get(0)) {
                  case "Госэкономика":
                  case "Общество":
                  case "Политика":
                  case "Происшествия":
                  case "Украина":
                  case "Футбол":
                    return true;
                  default:
                    return false;
                }
              })
              .limit(1000)
              .forEach(values -> {
                try {
                  csvPrinter.printRecord(values);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  @Test
  void prepareBalancedFewTopics() throws IOException {
    try (
            final FileInputStream in =
                    new FileInputStream("../../examples/src/main/resources/lenta/news_lenta.csv");
            final FileOutputStream out =
                    new FileOutputStream("../../examples/src/main/resources/lenta/balanced_few_topics_news_lenta.csv");
            final CSVPrinter csvPrinter = new CSVPrinter(new OutputStreamWriter(out), CSVFormat.DEFAULT)
    ) {
      final HashMap<String, Integer> counts = new HashMap<>();
      counts.put("Госэкономика", 0);
      counts.put("Общество", 0);
      counts.put("Политика", 0);
      counts.put("Происшествия", 0);
      counts.put("Украина", 0);
      counts.put("Футбол", 0);
      final int limit = 200;
      StreamSupport.stream(Spliterators.spliteratorUnknownSize(
              new CSVParser(new BufferedReader(new InputStreamReader(
                      in,
                      StandardCharsets.UTF_8
              )), CSVFormat.DEFAULT.withFirstRecordAsHeader()).iterator(),
              Spliterator.IMMUTABLE
      ), false)
              .filter(csvRecord -> {
                String topic = csvRecord.get(0);
                if (!counts.containsKey(topic) || counts.get(topic) == limit)
                  return false;
                counts.put(topic, counts.get(topic) + 1);
                return true;
              })
              .takeWhile(csvRecord -> !counts.entrySet().stream().allMatch(entry -> entry.getValue() == limit))
              .forEach(values -> {
                try {
                  csvPrinter.printRecord(values);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  @Test
  void testTopDispersion() throws Exception {
    final List<TextDocument> textDocuments = LentaCsvTextDocumentsReader
            .documents(new FileInputStream("../../examples/src/main/resources/lenta/news_lenta.csv"))
            .limit(10000)
            .collect(Collectors.toList());
    executeWithoutBlink(textDocuments, "stable-10000-0.csv");
    executeWithoutBlink(textDocuments, "stable-10000-1.csv");
    executeWithoutBlink(textDocuments, "stable-10000-2.csv");
    executeWithoutBlink(textDocuments, "stable-10000-3.csv");
    executeWithoutBlink(textDocuments, "stable-10000-4.csv");
    executeWithoutBlink(textDocuments, "stable-10000-5.csv");
    executeWithoutBlink(textDocuments, "stable-10000-6.csv");
    executeWithoutBlink(textDocuments, "stable-10000-7.csv");
    executeWithoutBlink(textDocuments, "stable-10000-8.csv");
    executeWithoutBlink(textDocuments, "stable-10000-9.csv");
  }

  @Test
  void testReplay500Of1000() throws Exception {
    final List<TextDocument> textDocuments = LentaCsvTextDocumentsReader
            .documents(new FileInputStream("../../examples/src/main/resources/lenta/news_lenta.csv"))
            .limit(1000)
            .collect(Collectors.toList());
    executeWithoutBlink(textDocuments, "few-topics-stable-1000-2.csv");
    executeWithoutBlink(textDocuments, "stable-1000-2.csv");
    executeWithBlink(textDocuments, "blink-replaying-500-of-1000.csv");
  }

  @Test
  void testReplay500Of1000WhenFewTopics() throws Exception {
    final List<TextDocument> textDocuments = LentaCsvTextDocumentsReader
            .documents(new FileInputStream("../../examples/src/main/resources/lenta/few_topics_news_lenta.csv"))
            .limit(1000)
            .collect(Collectors.toList());
    executeWithoutBlink(textDocuments, "few-topics-stable-1000.csv");
    executeWithoutBlink(textDocuments, "few-topics-stable-1000-2.csv");
    executeWithBlink(textDocuments, "few-topics-blink-replaying-500-of-1000.csv");
  }

  @Test
  void testReplay500Of1000WhenBalancedFewTopics() throws Exception {
    final List<TextDocument> textDocuments = LentaCsvTextDocumentsReader
            .documents(new FileInputStream("../../examples/src/main/resources/lenta/balanced_few_topics_news_lenta.csv"))
            .collect(Collectors.toList());
    executeWithoutBlink(textDocuments, "balanced_few-topics-stable-1000.csv");
    executeWithoutBlink(textDocuments, "balanced_few-topics-stable-1000-2.csv");
    executeWithBlink(textDocuments, "balanced_few-topics-blink-replaying-500-of-1000.csv");
  }

  @Test
  void testChm() throws Exception {
    final List<TextDocument> textDocuments = LentaCsvTextDocumentsReader
            .documents(new FileInputStream("../../examples/src/main/resources/lenta/chm_news_lenta.csv"))
            .collect(Collectors.toList());
    executeWithBlink(textDocuments, "chm-bootstrap-blinking6.csv");
    executeWithoutBlink(textDocuments, "chm-bootstrap-stable6.csv");
  }

  @Test
  public void testPredictionDataStream() throws Exception {
    StreamExecutionEnvironment env = streamExecutionEnvironment();
    final long blinkPeriodMillis = 7000;
    blinkAtMillis = System.currentTimeMillis() + blinkPeriodMillis;
    CollectSink.values.clear();
    final List<TextDocument> bootstrapped = LentaCsvTextDocumentsReader
            .documents(new FileInputStream("../../examples/src/main/resources/lenta/news_lenta.csv")).limit(1000)
            .collect(Collectors.toList());
    //predictionDataStream(env.fromCollection(bootstrapped).setParallelism(1))
    //        .map(value -> {
    //          if (Stream.of(value.topics())
    //                  .sorted(Comparator.comparing(Topic::probability).reversed())
    //                  .findFirst()
    //                  .get()
    //                  .name()
    //                  .equals("Зимние виды ") && Math.random() < 0.01) {
    //            throw new RuntimeException("blink");
    //          }
    //          {
    //            if (System.currentTimeMillis() > blinkAtMillis) {
    //              blinkAtMillis = System.currentTimeMillis() + blinkPeriodMillis;
    //              throw new RuntimeException("blink");
    //            }
    //          }
    //          return value;
    //        })
    //        .addSink(new CSVSink("blinking_predictions_bootstrap5.csv")).setParallelism(1);
    //env.execute();
    //CSVSink.openedCsvPrinter.close();
    //CSVSink.openedCsvPrinter = null;
    //CSVSink.topicNames = null;
    predictionDataStream(env.fromCollection(bootstrapped).setParallelism(1))
            .addSink(new CSVSink("stable_predictions-small-0.csv")).setParallelism(1);
    env.execute();
    CSVSink.openedCsvPrinter.close();
    CSVSink.openedCsvPrinter = null;
    CSVSink.topicNames = null;
    predictionDataStream(env.fromCollection(bootstrapped).setParallelism(1))
            .addSink(new CSVSink("stable_predictions-small-1.csv")).setParallelism(1);
    env.execute();
    CSVSink.openedCsvPrinter.close();
    CSVSink.openedCsvPrinter = null;
    CSVSink.topicNames = null;
    predictionDataStream(env.fromCollection(bootstrapped).setParallelism(1))
            .addSink(new CSVSink("stable_predictions-small-2.csv")).setParallelism(1);
    env.execute();
    CSVSink.openedCsvPrinter.close();
    CSVSink.openedCsvPrinter = null;
    CSVSink.topicNames = null;
    predictionDataStream(env.fromCollection(bootstrapped).setParallelism(1))
            .addSink(new CSVSink("stable_predictions-small-3.csv")).setParallelism(1);
    env.execute();
    CSVSink.openedCsvPrinter.close();
    CSVSink.openedCsvPrinter = null;
    CSVSink.topicNames = null;
    predictionDataStream(env.fromCollection(bootstrapped).setParallelism(1))
            .addSink(new CSVSink("stable_predictions-small-4.csv")).setParallelism(1);
    env.execute();
    CSVSink.openedCsvPrinter.close();
    CSVSink.openedCsvPrinter = null;
    CSVSink.topicNames = null;
    predictionDataStream(env.fromCollection(bootstrapped).setParallelism(1))
            .addSink(new CSVSink("stable_predictions-small-5.csv")).setParallelism(1);
    env.execute();
    CSVSink.openedCsvPrinter.close();
  }

  @NotNull
  private StreamExecutionEnvironment streamExecutionEnvironment() throws IOException {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(10000);
    env.setStateBackend(new RocksDBStateBackend(new File("rocksdb").toURI(), true));
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2000, 100));
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
    return env;
  }

  static boolean blinked = false;

  private void executeWithBlink(List<TextDocument> textDocuments, String fileName) throws Exception {
    StreamExecutionEnvironment env = streamExecutionEnvironment();
    final long blinkPeriodMillis = 7000;
    blinkAtMillis = System.currentTimeMillis() + blinkPeriodMillis;
    predictionDataStream(env.fromCollection(textDocuments).setParallelism(1))
            .map(value -> {
              {
                if (value.tfIdf().number() > 500 && !blinked) {
                  blinked = true;
                  throw new RuntimeException("blink");
                }
              }
              return value;
            })
            .addSink(new CSVSink(fileName)).setParallelism(1);
    env.execute();
    CSVSink.openedCsvPrinter.close();
    CSVSink.openedCsvPrinter = null;
    CSVSink.topicNames = null;
  }

  private void executeWithoutBlink(List<TextDocument> textDocuments, String fileName) throws Exception {
    StreamExecutionEnvironment env = streamExecutionEnvironment();
    predictionDataStream(env.fromCollection(textDocuments).setParallelism(1))
            .addSink(new CSVSink(fileName)).setParallelism(1);
    env.execute();
    CSVSink.openedCsvPrinter.close();
    CSVSink.openedCsvPrinter = null;
    CSVSink.topicNames = null;
  }

  private DataStream<Prediction> predictionDataStream(DataStreamSource<TextDocument> source) {
    return FlinkBench.predictionDataStream(new ExamplesTopicPredictor(), source);
  }

  private static class CollectSink implements SinkFunction<Prediction> {
    static final ArrayList<Prediction> values = new ArrayList<>();

    @Override
    public void invoke(Prediction value, Context context) throws Exception {
      values.add(value);
    }
  }

  private static class CSVSink implements SinkFunction<Prediction> {
    private final String fileName;
    static CSVPrinter openedCsvPrinter;
    static private String[] topicNames;

    private CSVSink(String fileName) {
      this.fileName = fileName;
      System.out.println("sink");
    }

    @Override
    public synchronized void invoke(Prediction prediction, Context context) throws Exception {
      if (topicNames == null) {
        topicNames = Stream.of(prediction.topics()).map(Topic::name).toArray(String[]::new);
        for (final String topicName : topicNames) {
          csvPrinter().print(topicName);
        }
        csvPrinter().println();
      }
      csvPrinter().print(prediction.tfIdf().document());
      if (prediction.topics().length != topicNames.length) {
        throw new RuntimeException("different topics number");
      }
      int i = 0;
      for (final Topic topic : prediction.topics()) {
        if (!topic.name().equals(topicNames[i])) {
          throw new RuntimeException("inconsistent topics order");
        }
        i++;
        csvPrinter().print(topic.probability());
      }
      csvPrinter().println();
      if (prediction.tfIdf().number() % 1000 == 0) {
        System.out.println(prediction.tfIdf().number());
      }
    }

    private CSVPrinter csvPrinter() {
      if (openedCsvPrinter != null) {
        return openedCsvPrinter;
      }
      try {
        openedCsvPrinter = new CSVPrinter(new FileWriter(fileName), CSVFormat.DEFAULT);
        openedCsvPrinter.print("document");
        return openedCsvPrinter;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
