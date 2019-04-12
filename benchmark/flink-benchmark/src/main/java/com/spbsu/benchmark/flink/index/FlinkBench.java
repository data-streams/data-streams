package com.spbsu.benchmark.flink.index;

import com.spbsu.benchmark.flink.index.ops.IndexFunction;
import com.spbsu.benchmark.flink.index.ops.KryoSocketSink;
import com.spbsu.benchmark.flink.index.ops.KryoSocketSource;
import com.spbsu.benchmark.flink.index.ops.OrderEnforcer;
import com.spbsu.benchmark.flink.index.ops.TwoPCKryoSocketSink;
import com.spbsu.benchmark.flink.index.ops.WikipediaPageToWordPositions;
import com.spbsu.flamestream.example.benchmark.WikiBenchStand;
import com.spbsu.flamestream.example.benchmark.GraphDeployer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.jooq.lambda.Unchecked;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FlinkBench {
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
    WikiBenchStand wikiBenchStand = new WikiBenchStand(benchConfig);
    wikiBenchStand.run(new GraphDeployer() {
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
        final SinkFunction<Result> sinkFunction;
        if (guarantees.equals("EXACTLY_ONCE")) {
          sinkFunction = new TwoPCKryoSocketSink(
                  wikiBenchStand.benchHost,
                  wikiBenchStand.rearPort,
                  environment.getConfig()
          );
        } else {
          sinkFunction = new KryoSocketSink(wikiBenchStand.benchHost, wikiBenchStand.rearPort);
        }

        if (guarantees.equals("EXACTLY_ONCE") || guarantees.equals("AT_LEAST_ONCE")) {
          final int millisBetweenCommits = deployerConfig.getInt("millis-between-commits");
          environment.enableCheckpointing(millisBetweenCommits);
          environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(1);
          environment.getCheckpointConfig()
                  .setCheckpointingMode(guarantees.equals("EXACTLY_ONCE") ? CheckpointingMode.EXACTLY_ONCE : CheckpointingMode.AT_LEAST_ONCE);
        }

        final String rocksDbPath = deployerConfig.getString("rocksdb-path");
        final StateBackend backend = new FsStateBackend("file:///" + rocksDbPath, true);
        environment.setStateBackend(backend);


        environment
                .addSource(new KryoSocketSource(wikiBenchStand.benchHost, wikiBenchStand.frontPort))
                .setParallelism(parallelism)
                .shuffle()
                .flatMap(new WikipediaPageToWordPositions())
                .setParallelism(parallelism)
                .keyBy(0)
                .process(new OrderEnforcer())
                .keyBy(0)
                .map(new IndexFunction())
                .setParallelism(parallelism)
                //.keyBy((KeySelector<Result, Integer>) value
                //        -> IndexItemInLong.pageId(value.wordIndexAdd().positions()[0]))
                //.process(new TotalOrderEnforcer())
                .setParallelism(parallelism)
                .addSink(sinkFunction)
                .setParallelism(parallelism);
        new Thread(Unchecked.runnable(environment::execute)).start();

      }

      @Override
      public void close() {
        // It will close itself on completion
      }
    });
    System.exit(0);
  }
}
