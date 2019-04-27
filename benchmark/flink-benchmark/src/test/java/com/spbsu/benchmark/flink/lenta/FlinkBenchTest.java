package com.spbsu.benchmark.flink.lenta;

import com.spbsu.flamestream.example.bl.text_classifier.LentaCsvTextDocumentsReader;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;

public class FlinkBenchTest {
  @Test
  public void testPredictionDataStream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setBufferTimeout(0);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(2);
    env.enableCheckpointing(2000);
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1);
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(30000, 1000));
    try (final FileInputStream inputStream = new FileInputStream(
            "../../examples/src/main/resources/lenta/lenta-ru-news.csv"
    )) {
      final List<TextDocument> documents =
              LentaCsvTextDocumentsReader.documents(inputStream).collect(Collectors.toList());
      final Map<String, Prediction> withoutRestart = new HashMap<>(), withRestart = new HashMap<>();
      DataStreamUtils.collect(predictionDataStream(env, documents)).forEachRemaining(
              prediction -> withoutRestart.put(prediction.tfIdf().document(), prediction)
      );
      DataStreamUtils.collect(predictionDataStream(env, documents)).forEachRemaining(
              prediction -> withRestart.put(prediction.tfIdf().document(), prediction)
      );
      assertEquals(withoutRestart.size(), withRestart.size());
      assertEquals(withoutRestart.keySet(), withRestart.keySet());
    }
    //for (final Map.Entry<String, Prediction> predictionEntry : withoutRestart.entrySet()) {
    //  assertEquals(
    //          topTopics(predictionEntry.getValue(), 5).stream().map(Topic::name).collect(Collectors.toList()),
    //          topTopics(withRestart.get(predictionEntry.getKey()), 5).stream()
    //                  .map(Topic::name)
    //                  .collect(Collectors.toList())
    //  );
    //}

    //try (final FileWriter outputFile = new FileWriter("lenta.csv")) {
    //  for (final Prediction prediction : CollectSink.values) {
    //    outputFile.append(prediction.tfIdf().document());
    //    for (final Topic topic : topTopics(prediction, 5)) {
    //      outputFile.append(',');
    //      outputFile.append(topic.name());
    //      outputFile.append(',');
    //      outputFile.append(Double.toString(topic.probability()));
    //    }
    //    outputFile.append('\n');
    //  }
    //}
  }

  @NotNull
  private List<Topic> topTopics(Prediction prediction, int maxSize) {
    return Stream.of(prediction.topics())
            .sorted(Comparator.comparing(Topic::probability).reversed())
            .limit(maxSize)
            .collect(Collectors.toList());
  }

  private DataStream<Prediction> predictionDataStream(StreamExecutionEnvironment env, List<TextDocument> documents) {
    return FlinkBench.predictionDataStream(
            "../../examples/src/main/resources/cnt_vectorizer",
            "../../examples/src/main/resources/classifier_weights",
            env.fromCollection(documents)
    );
  }
}
