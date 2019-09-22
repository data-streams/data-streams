package com.spbsu.flamestream.runtime;

import akka.actor.ActorSystem;
import akka.actor.CoordinatedShutdown;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.master.acker.LocalAcker;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.IntConsumer;

import static com.spbsu.flamestream.runtime.ConfigureFromEnv.*;

public class WorkerApplication implements Runnable {
  private final Logger log = LoggerFactory.getLogger(WorkerApplication.class);
  private final SystemConfig systemConfig;
  private final WorkerConfig workerConfig;

  @Nullable
  private ActorSystem system = null;

  static {
    try {
      Class.forName("org.agrona.concurrent.SleepingIdleStrategy");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public WorkerApplication(WorkerConfig workerConfig, SystemConfig systemConfig) {
    this.workerConfig = workerConfig;
    this.systemConfig = systemConfig;
  }

  public static void main(String... args) {
    final String[] localAddressHostAndPort = System.getenv("LOCAL_ADDRESS").split(":");
    LocalAcker.Builder localAckerBuilder = new LocalAcker.Builder();
    configureFromEnv(localAckerBuilder::flushDelayInMillis, "LOCAL_ACKER_FLUSH_DELAY_IN_MILLIS");
    configureFromEnv(localAckerBuilder::flushCount, "LOCAL_ACKER_FLUSH_COUNT");
    final WorkerConfig.Builder configBuilder = new WorkerConfig.Builder();
    configureFromEnv(configBuilder::snapshotPath, "SNAPSHOT_PATH");
    configureFromEnv(configBuilder::guarantees, Guarantees::valueOf, "GUARANTEES");
    final SystemConfig.Builder systemConfigBuilder = new SystemConfig.Builder().localAckerBuilder(localAckerBuilder);
    configureFromEnv(systemConfigBuilder::defaultMinimalTime, "DEFAULT_MINIMAL_TIME");
    configureFromEnv(systemConfigBuilder::millisBetweenCommits, "MILLIS_BETWEEN_COMMITS");
    configureFromEnv(systemConfigBuilder::maxElementsInGraph, "MAX_ELEMENTS_IN_GRAPH");
    configureFromEnv(systemConfigBuilder::barrierDisabled, Boolean::parseBoolean, "BARRIER_DISABLED");
    configureFromEnv(
            (IntConsumer) ackersNumber ->
                    systemConfigBuilder
                            .workersResourcesDistributor(new SystemConfig.WorkersResourcesDistributor.Enumerated(
                                    "flamestream-benchmarks-worker-",
                                    ackersNumber
                            )),
            "ACKERS_NUMBER"
    );
    configureFromEnv(systemConfigBuilder::ackerWindow, "ACKER_WINDOW");
    final WorkerConfig config = configBuilder.build(
            System.getenv("ID"),
            new InetSocketAddress(localAddressHostAndPort[0], Integer.parseInt(localAddressHostAndPort[1])),
            System.getenv("ZK_STRING")
    );
    new WorkerApplication(config, systemConfigBuilder.build()).run();
  }

  @Override
  public void run() {
    log.info("Starting worker with workerConfig '{}'", workerConfig);

    final Map<String, String> props = new HashMap<>();
    props.put("akka.remote.artery.canonical.hostname", workerConfig.localAddress.getHostName());
    props.put("akka.remote.artery.canonical.port", String.valueOf(workerConfig.localAddress.getPort()));
    props.put("akka.remote.artery.bind.hostname", "0.0.0.0");
    props.put("akka.remote.artery.bind.port", String.valueOf(workerConfig.localAddress.getPort()));
    try {
      final File shm = new File(("/dev/shm"));
      if (shm.exists() && shm.isDirectory()) {
        final String aeronDir = "/dev/shm/aeron-" + workerConfig.id;
        FileUtils.deleteDirectory(new File(aeronDir));
        props.put("akka.remote.artery.advanced.aeron.aeron-dir", aeronDir);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    final Config config = ConfigFactory.parseMap(props).withFallback(ConfigFactory.load("remote"));
    this.system = ActorSystem.create("worker", config);
    system.actorOf(
            StartupWatcher.props(workerConfig.id, workerConfig.zkString, workerConfig.snapshotPath, systemConfig),
            "watcher"
    );

    CoordinatedShutdown.get(system).addJvmShutdownHook(() -> {
      try {
        Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
      } catch (IOException e) {
        log.error("Something went wrong during trace flush", e);
      }
    });
  }

  public void close() {
    try {
      if (system != null) {
        Await.ready(system.terminate(), Duration.Inf());
      }
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public static class WorkerConfig {
    private final String id;
    private final InetSocketAddress localAddress;
    private final String zkString;

    private final String snapshotPath;
    private final Guarantees guarantees;

    private WorkerConfig(
            String id,
            InetSocketAddress localAddress,
            String zkString,
            String snapshotPath,
            Guarantees guarantees
    ) {
      this.guarantees = guarantees;
      this.id = id;
      this.localAddress = localAddress;
      this.zkString = zkString;
      this.snapshotPath = snapshotPath;
    }

    @Override
    public String toString() {
      return "WorkerConfig{" +
              "id='" + id + '\'' +
              ", localAddress=" + localAddress +
              ", zkString='" + zkString + '\'' +
              ", snapshotPath='" + snapshotPath + '\'' +
              ", guarantees=" + guarantees +
              '}';
    }

    public static class Builder {
      private String snapshotPath = null;
      private Guarantees guarantees = Guarantees.AT_MOST_ONCE;

      public Builder snapshotPath(String snapshotPath) {
        this.snapshotPath = snapshotPath;
        return this;
      }

      public Builder guarantees(Guarantees guarantees) {
        this.guarantees = guarantees;
        return this;
      }

      public WorkerConfig build(String id, InetSocketAddress localAddress, String zkString) {
        return new WorkerConfig(
                id,
                localAddress,
                zkString,
                snapshotPath,
                guarantees
        );
      }
    }
  }

  @SuppressWarnings("unused")
  public enum Guarantees {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE
  }
}
