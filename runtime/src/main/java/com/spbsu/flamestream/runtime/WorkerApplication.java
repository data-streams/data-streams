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

public class WorkerApplication implements Runnable {
  private final Logger log = LoggerFactory.getLogger(WorkerApplication.class);
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

  @SuppressWarnings("WeakerAccess")
  public WorkerApplication(WorkerConfig workerConfig) {
    this.workerConfig = workerConfig;
  }

  public static void main(String... args) {
    final String[] localAddressHostAndPort = System.getenv("LOCAL_ADDRESS").split(":");
    LocalAcker.Builder localAckerBuilder = new LocalAcker.Builder();
    if (System.getenv().containsKey("LOCAL_ACKER_FLUSH_DELAY_IN_MILLIS")) {
      localAckerBuilder.flushDelayInMillis(Integer.parseInt(System.getenv("LOCAL_ACKER_FLUSH_DELAY_IN_MILLIS")));
    }
    if (System.getenv().containsKey("LOCAL_ACKER_FLUSH_COUNT")) {
      localAckerBuilder.flushCount(Integer.parseInt(System.getenv("LOCAL_ACKER_FLUSH_COUNT")));
    }
    WorkerConfig.Builder configBuilder = new WorkerConfig.Builder()
            .snapshotPath(System.getenv("SNAPSHOT_PATH"))
            .guarantees(Guarantees.valueOf(System.getenv("GUARANTEES")))
            .defaultMinimalTime(Integer.parseInt(System.getenv("DEFAULT_MINIMAL_TIME")))
            .millisBetweenCommits(Integer.parseInt(System.getenv("MILLIS_BETWEEN_COMMITS")))
            .maxElementsInGraph(Integer.parseInt(System.getenv("MAX_ELEMENTS_IN_GRAPH")))
            .acking(SystemConfig.Acking.valueOf(System.getenv("ACKING")))
            .barrierDisabled(Boolean.parseBoolean(System.getenv("BARRIER_DISABLED")))
            .localAckerBuilder(localAckerBuilder);
    final WorkerConfig config = configBuilder
            .build(
                    System.getenv("ID"),
                    new InetSocketAddress(localAddressHostAndPort[0], Integer.parseInt(localAddressHostAndPort[1])),
                    System.getenv("ZK_STRING")
            );
    new WorkerApplication(config).run();
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
        props.put("akka.remote.artery.advanced.aeron-dir", aeronDir);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    final Config config = ConfigFactory.parseMap(props).withFallback(ConfigFactory.load("remote"));
    this.system = ActorSystem.create("worker", config);
    final SystemConfig systemConfig = new SystemConfig(
            workerConfig.maxElementsInGraph,
            workerConfig.millisBetweenCommits,
            workerConfig.defaultMinimalTime,
            workerConfig.acking,
            workerConfig.barrierDisabled,
            workerConfig.localAckerBuilder
    );
    //noinspection ConstantConditions
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

    private final int maxElementsInGraph;
    private final int millisBetweenCommits;
    private final int defaultMinimalTime;
    private final SystemConfig.Acking acking;
    private final boolean barrierDisabled;
    private final LocalAcker.Builder localAckerBuilder;

    private WorkerConfig(
            String id,
            InetSocketAddress localAddress,
            String zkString,
            String snapshotPath,
            Guarantees guarantees,
            int maxElementsInGraph,
            int millisBetweenCommits,
            int defaultMinimalTime,
            SystemConfig.Acking acking,
            boolean barrierDisabled,
            LocalAcker.Builder localAckerBuilder
    ) {
      this.guarantees = guarantees;
      this.id = id;
      this.localAddress = localAddress;
      this.zkString = zkString;
      this.snapshotPath = snapshotPath;
      this.maxElementsInGraph = maxElementsInGraph;
      this.millisBetweenCommits = millisBetweenCommits;
      this.defaultMinimalTime = defaultMinimalTime;
      this.acking = acking;
      this.barrierDisabled = barrierDisabled;
      this.localAckerBuilder = localAckerBuilder;
    }

    @Override
    public String toString() {
      return "WorkerConfig{" +
              "id='" + id + '\'' +
              ", localAddress=" + localAddress +
              ", zkString='" + zkString + '\'' +
              ", snapshotPath='" + snapshotPath + '\'' +
              ", guarantees=" + guarantees +
              ", maxElementsInGraph=" + maxElementsInGraph +
              ", millisBetweenCommits=" + millisBetweenCommits +
              ", defaultMinimalTime=" + defaultMinimalTime +
              ", barrierDisabled=" + barrierDisabled +
              ", acking=" + acking +
              '}';
    }

    public interface Factory {
      WorkerConfig create(String name, InetSocketAddress localAddress, String zkString);
    }

    public static class Builder {
      private String snapshotPath = null;
      private Guarantees guarantees = Guarantees.AT_MOST_ONCE;

      private int maxElementsInGraph = 500;
      private int millisBetweenCommits = 100;
      private int defaultMinimalTime = 0;
      private SystemConfig.Acking acking = SystemConfig.Acking.CENTRALIZED;
      private boolean barrierDisabled = false;
      private LocalAcker.Builder localAckerBuilder = new LocalAcker.Builder();

      public Builder snapshotPath(String snapshotPath) {
        this.snapshotPath = snapshotPath;
        return this;
      }

      public Builder guarantees(Guarantees guarantees) {
        this.guarantees = guarantees;
        return this;
      }

      public Builder maxElementsInGraph(int maxElementsInGraph) {
        this.maxElementsInGraph = maxElementsInGraph;
        return this;
      }

      public Builder millisBetweenCommits(int millisBetweenCommits) {
        this.millisBetweenCommits = millisBetweenCommits;
        return this;
      }

      public Builder defaultMinimalTime(int defaultMinimalTime) {
        this.defaultMinimalTime = defaultMinimalTime;
        return this;
      }

      public Builder acking(SystemConfig.Acking acking) {
        this.acking = acking;
        return this;
      }

      public WorkerConfig build(String id, InetSocketAddress localAddress, String zkString) {
        return new WorkerConfig(
                id,
                localAddress,
                zkString,
                snapshotPath,
                guarantees,
                maxElementsInGraph,
                millisBetweenCommits,
                defaultMinimalTime,
                acking,
                barrierDisabled,
                localAckerBuilder
        );
      }

      public Builder barrierDisabled(boolean barrierDisabled) {
        this.barrierDisabled = barrierDisabled;
        return this;
      }

      public Builder localAckerBuilder(LocalAcker.Builder localAckerBuilder) {
        this.localAckerBuilder = localAckerBuilder;
        return this;
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
