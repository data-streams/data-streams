package com.spbsu.benchmark.flink.lenta;

import com.spbsu.benchmark.flink.lenta.ops.WordCountFunction;
import com.spbsu.benchmark.flink.lenta.ops.KryoSocketSink;
import com.spbsu.benchmark.flink.lenta.ops.KryoSocketSource;
import com.spbsu.benchmark.flink.lenta.ops.TwoPCKryoSocketSink;
import com.spbsu.flamestream.example.benchmark.GraphDeployer;
import com.spbsu.flamestream.example.benchmark.LentaBenchStand;
import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordEntry;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.Classifier;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.IDFObjectCompleteFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.jooq.lambda.Unchecked;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FlinkBench {
  interface SerializableTopicsPredictor extends TopicsPredictor, Serializable {
  }

  static class MainPredictor implements SerializableTopicsPredictor {
    public static final SklearnSgdPredictor predictor = new SklearnSgdPredictor(
            "/opt/flamestream/cnt_vectorizer",
            "/opt/flamestream/classifier_weights"
    );

    static {
      predictor.init();
    }

    @Override
    public Topic[] predict(Document document) {
      return predictor.predict(document);
    }
  }

  public static void main(String[] args) throws Exception {
    final Config benchConfig;
    final Config deployerConfig;
    if (args.length == 2) {
      benchConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[0]))).getConfig("benchmark");
      deployerConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[1]))).getConfig("deployer");
    } else {
      benchConfig = ConfigFactory.load("flink-bench.conf").getConfig("benchmark");
      deployerConfig = ConfigFactory.load("flink-deployer.conf").getConfig("deployer");
    }
    LentaBenchStand benchStand = new LentaBenchStand(benchConfig);
    benchStand.run(new GraphDeployer() {
      @Override
      public void deploy() {
        final int parallelism = deployerConfig.getInt("parallelism");
        final StreamExecutionEnvironment environment;
        if (deployerConfig.hasPath("remote")) {
          environment = StreamExecutionEnvironment.createRemoteEnvironment(
                  deployerConfig.getString("remote.manager-hostname"),
                  deployerConfig.getInt("remote.manager-port"),
                  parallelism,
                  deployerConfig.getString("remote.uber-jar")
          );
        } else {
          environment = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
        }
        environment.setBufferTimeout(0);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        final String guarantees = deployerConfig.getString("guarantees");
        final SinkFunction<Prediction> sinkFunction;
        if (guarantees.equals("EXACTLY_ONCE")) {
          sinkFunction = new TwoPCKryoSocketSink(
                  benchStand.benchHost,
                  benchStand.rearPort,
                  environment.getConfig()
          );
        } else {
          sinkFunction = new KryoSocketSink(benchStand.benchHost, benchStand.rearPort);
        }

        if (guarantees.equals("EXACTLY_ONCE") || guarantees.equals("AT_LEAST_ONCE")) {
          final int millisBetweenCommits = deployerConfig.getInt("millis-between-commits");
          environment.enableCheckpointing(millisBetweenCommits);
          environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(1);
          environment.getCheckpointConfig()
                  .setCheckpointingMode(guarantees.equals("EXACTLY_ONCE") ? CheckpointingMode.EXACTLY_ONCE : CheckpointingMode.AT_LEAST_ONCE);
        }

        final String rocksDbPath = deployerConfig.getString("rocksdb-path");
        environment.setStateBackend(new FsStateBackend(new File(rocksDbPath).toURI(), true));

        predictionDataStream(
                new MainPredictor(),
                environment
                        .addSource(new KryoSocketSource(benchStand.benchHost, benchStand.frontPort))
                        .setParallelism(parallelism)
        ).addSink(sinkFunction);
        new Thread(Unchecked.runnable(environment::execute)).start();
      }

      @Override
      public void close() {
        // It will close itself on completion
      }
    });
    System.exit(0);
  }

  static DataStream<Prediction> predictionDataStream(
          SerializableTopicsPredictor topicsPredictor,
          DataStream<TextDocument> source
  ) {
    final SingleOutputStreamOperator<TfObject> splitterTf = source
            .shuffle()
            .map(TfObject::ofText);
    return splitterTf
            .<WordEntry>flatMap((tfObject, out) -> {
              for (final String word : tfObject.counts().keySet()) {
                out.collect(new WordEntry(
                        word,
                        tfObject.document(),
                        tfObject.counts().size(),
                        tfObject.partitioning()
                ));
              }
            }).returns(WordEntry.class)
            .keyBy(WordEntry::word)
            .map(new WordCountFunction())
            .keyBy(WordCounter::document)
            .flatMap(new IdfObjectCompleteFilter())
            .connect(splitterTf)
            .keyBy(IdfObject::document, TfObject::document)
            .flatMap(new RichCoFlatMapFunction<IdfObject, TfObject, TfIdfObject>() {
              @Override
              public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                storedIdf = getRuntimeContext().getState(new ValueStateDescriptor<>(
                        "idf",
                        new GenericTypeInfo<>(IdfObject.class)
                ));
                storedTf = getRuntimeContext().getState(new ValueStateDescriptor<>(
                        "tf",
                        new GenericTypeInfo<>(TfObject.class)
                ));
              }

              private transient ValueState<IdfObject> storedIdf;
              private transient ValueState<TfObject> storedTf;

              @Override
              public void flatMap1(IdfObject value, Collector<TfIdfObject> out) throws Exception {
                if (storedTf.value() == null) {
                  storedIdf.update(value);
                } else {
                  out.collect(new TfIdfObject(storedTf.value(), value));
                  storedTf.update(null);
                }
              }

              @Override
              public void flatMap2(TfObject value, Collector<TfIdfObject> out) throws Exception {
                if (storedIdf.value() == null) {
                  storedTf.update(value);
                } else {
                  out.collect(new TfIdfObject(value, storedIdf.value()));
                  storedIdf.update(null);
                }
              }
            })
            .map(tfIdfObject -> new Classifier(topicsPredictor).apply(tfIdfObject));
  }

  private static class IdfObjectCompleteFilter extends RichFlatMapFunction<WordCounter, IdfObject> {
    IDFObjectCompleteFilter buffer = null;

    @Override
    public void open(Configuration parameters) {
      buffer = new IDFObjectCompleteFilter();
      buffer.init();
    }

    @Override
    public void flatMap(WordCounter wordCounter, Collector<IdfObject> out) {
      buffer.apply(wordCounter).forEach(out::collect);
    }
  }
}
