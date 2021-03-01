package com.spbsu.benchmark.flink.roundrobin;

import com.spbsu.benchmark.flink.roundrobin.ops.IntermediateVertex;
import com.spbsu.benchmark.flink.roundrobin.ops.SimpleSink;
import com.spbsu.benchmark.flink.roundrobin.ops.SimpleSource;
import com.spbsu.flamestream.example.benchmark.GraphDeployer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jooq.lambda.Unchecked;

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
      benchConfig = ConfigFactory.load("flink-rr-bench.conf").getConfig("benchmark");
      deployerConfig = ConfigFactory.load("flink-rr-deployer.conf").getConfig("deployer");
    }

    final WatermarkBenchStand wikiBenchStand = new WatermarkBenchStand(benchConfig);
    final int parallelism = deployerConfig.getInt("parallelism");

    wikiBenchStand.run(new GraphDeployer() {
      @Override
      public void deploy() {
        final StreamExecutionEnvironment environment;
        if (deployerConfig.hasPath("remote")) {
          environment = StreamExecutionEnvironment.createRemoteEnvironment(
                  deployerConfig.getString("remote.manager-hostname"),
                  deployerConfig.getInt("remote.manager-port"),
                  parallelism,
                  deployerConfig.getString("remote.uber-jar")
          );
        } else {
          final Configuration config = new Configuration();
          config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
          environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
          environment.setParallelism(5);
        }
        environment.setBufferTimeout(0);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        SingleOutputStreamOperator<Integer> operator = environment.addSource(new SimpleSource(
                wikiBenchStand.benchHost,
                wikiBenchStand.frontPort,
                benchConfig.getInt("watermarks-period")
        ));
        for (int i = 0; i < 30; i++) {
          operator = operator.setParallelism(parallelism).shuffle().map(new IntermediateVertex());
        }
        operator.setParallelism(parallelism).shuffle().addSink(new SimpleSink(
                wikiBenchStand.benchHost,
                wikiBenchStand.rearPort
        ));
        new Thread(Unchecked.runnable(environment::execute)).start();
      }

      @Override
      public void close() {

      }
    }, parallelism);

    System.exit(0);
  }
}
