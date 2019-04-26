package com.spbsu.benchmark.flink.lenta.ops;

import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordEntry;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * User: Artem
 * Date: 04.01.2018
 */
public class IndexFunction extends RichMapFunction<WordEntry, WordCounter> {
  private transient ValueState<WordCounter> checkpointedState;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.checkpointedState = getRuntimeContext().getState(new ValueStateDescriptor<>(
            "index_state",
            new GenericTypeInfo<>(WordCounter.class)
    ));
    new Thread(() -> {
      while (true) {
        try {
          Runtime.getRuntime()
                  .exec("find /opt/flink/data -mindepth 2 -type d -cmin +0.2 -exec rm -rf {} +")
                  .waitFor();
          TimeUnit.SECONDS.sleep(10);
        } catch (IOException | InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }).start();
  }

  @Override
  public WordCounter map(WordEntry value) throws IOException {
    WordCounter wordCounter = checkpointedState.value();
    if (wordCounter == null) {
      wordCounter = new WordCounter(value, 0);
    }
    wordCounter = new WordCounter(value, wordCounter.count() + 1);
    checkpointedState.clear();
    checkpointedState.update(wordCounter);
    return wordCounter;
  }
}
