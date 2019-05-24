package com.spbsu.benchmark.flink.lenta;

import com.expleague.commons.math.vectors.Vec;
import com.spbsu.benchmark.flink.lenta.ops.KryoSocketSink;
import com.spbsu.benchmark.flink.lenta.ops.KryoSocketSource;
import com.spbsu.benchmark.flink.lenta.ops.TwoPCKryoSocketSink;
import com.spbsu.benchmark.flink.lenta.ops.WordCountFunction;
import com.spbsu.flamestream.example.benchmark.GraphDeployer;
import com.spbsu.flamestream.example.benchmark.LentaBenchStand;
import com.spbsu.flamestream.example.bl.text_classifier.model.ClassifierState;
import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordEntry;
import com.spbsu.flamestream.example.bl.text_classifier.ops.Classifier;
import com.spbsu.flamestream.example.bl.text_classifier.ops.IDFObjectCompleteFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.CountVectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.OnlineModel;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.TextUtils;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLProximal;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
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
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FlinkBench {
  static class MainVectorizer implements Vectorizer, Serializable {
    static final private Vectorizer vectorizer = new CountVectorizer("/opt/flamestream/cnt_vectorizer");

    static {
      vectorizer.init();
    }

    @Override
    public Vec vectorize(Document document) {
      return vectorizer.vectorize(document);
    }

    @Override
    public int dim() {
      return vectorizer.dim();
    }
  }

  static class MainOnlineModel implements OnlineModel, Serializable {
    static final private OnlineModel onlineModel = FTRLProximal.builder()
            .alpha(132)
            .beta(0.1)
            .lambda1(0.5)
            .lambda2(0.095)
            .build(TextUtils.readTopics("/opt/flamestream/classifier_weights"));

    @Override
    public ModelState step(DataPoint trainingPoint, ModelState prevState) {
      return onlineModel.step(trainingPoint, prevState);
    }

    @Override
    public Topic[] predict(ModelState state, Vec vec) {
      return onlineModel.predict(state, vec);
    }

    @Override
    public int classes() {
      return onlineModel.classes();
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

        try {
          environment.setStateBackend(new FsStateBackend(
                  new File(deployerConfig.getString("rocksdb-path")).toURI(),
                  true
          ));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        predictionDataStream(
                new MainVectorizer(),
                new MainOnlineModel(),
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

  static <
          SerializableVectorizer extends Vectorizer & Serializable,
          SerializableOnlineModel extends OnlineModel & Serializable
          > DataStream<Prediction> predictionDataStream(
          SerializableVectorizer vectorizer,
          SerializableOnlineModel onlineModel,
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
            .map(new RichMapFunction<>() {
              private transient ValueState<ClassifierState> classifierState;

              @Override
              public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                classifierState = getRuntimeContext().getState(new ValueStateDescriptor<>(
                        "classifierState",
                        new GenericTypeInfo<>(ClassifierState.class)
                ));
              }

              @Override
              public Prediction map(TfIdfObject tfIdfObject) throws Exception {
                Classifier classifier = new Classifier(vectorizer, onlineModel);
                if (classifierState.value() == null) {
                  classifierState.update(classifier.initialState(tfIdfObject));
                }
                Topic[] topics = classifier.topics(classifierState.value(), tfIdfObject);
                classifierState.update(classifier.newState(classifierState.value(), tfIdfObject));
                return new Prediction(tfIdfObject, topics);
              }
            });
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
