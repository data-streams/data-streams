package com.spbsu.benchmark.flink.roundrobin;

import com.spbsu.benchmark.flink.roundrobin.ops.IntermediateVertex;
import com.spbsu.benchmark.flink.roundrobin.ops.SimpleSink;
import com.spbsu.benchmark.flink.roundrobin.ops.SimpleSource;
import com.spbsu.flamestream.example.benchmark.GraphDeployer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jooq.lambda.Unchecked;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.Collectors;

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
      environment = StreamExecutionEnvironment.createLocalEnvironment();
      environment.setParallelism(5);
    }
    environment.setBufferTimeout(0);
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    environment.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

    double[] sleeps = new double[]{1.0, 0.1, 0.01, 0.001};
    for (double sleep : sleeps) {
      final WatermarkBenchStand wikiBenchStand = new WatermarkBenchStand(benchConfig, sleep);

      final long[] latencies = wikiBenchStand.run(getDeployer(
              environment,
              benchConfig,
              wikiBenchStand,
              parallelism
      ), parallelism);

      try (final PrintWriter bw = new PrintWriter(Files.newBufferedWriter(
              Path.of(sleep + "csv"),
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE,
              StandardOpenOption.TRUNCATE_EXISTING
      ))) {
        bw.println(Arrays.stream(latencies).boxed().map(Object::toString).collect(Collectors.joining(",")));
      }
    }

    System.exit(0);
  }

  public static GraphDeployer getDeployer(
          StreamExecutionEnvironment environment,
          Config benchConfig,
          WatermarkBenchStand wikiBenchStand,
          int parallelism
  ) {
    return new GraphDeployer() {
      @Override
      public void deploy() {
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
    };
  }
}
