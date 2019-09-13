package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.HashGroup;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.config.ZookeeperWorkersNode;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.server.ZooKeeperApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LocalClusterRuntime implements FlameRuntime {
  private static final Logger LOG = LoggerFactory.getLogger(LocalClusterRuntime.class);

  private final ZooKeeperApplication zooKeeperApplication;
  private final RemoteRuntime remoteRuntime;
  private final Set<WorkerApplication> workers = new HashSet<>();
  private final String zkString;
  private final CuratorFramework curator;

  public LocalClusterRuntime(int parallelism, SystemConfig systemConfig) {
    final List<Integer> ports;
    try {
      ports = new ArrayList<>(freePorts(parallelism + 1));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.zooKeeperApplication = new ZooKeeperApplication(ports.get(0));
    zooKeeperApplication.run();

    zkString = "localhost:" + ports.get(0);
    LOG.info("ZK string: {}", zkString);

    final WorkerApplication.WorkerConfig.Builder builder = new WorkerApplication.WorkerConfig.Builder();
    for (int i = 0; i < parallelism; i++) {
      final String name = "worker" + i;
      final InetSocketAddress address = new InetSocketAddress("localhost", ports.get(i + 1));

      final WorkerApplication worker = new WorkerApplication(builder.build(name, address, zkString), systemConfig);
      workers.add(worker);
      worker.run();
    }

    this.curator = CuratorFrameworkFactory.newClient(
            zkString,
            new ExponentialBackoffRetry(1000, 3)
    );
    curator.start();
    final ZookeeperWorkersNode workersNode = new ZookeeperWorkersNode(curator, "/workers");
    while (workersNode.workers().size() != parallelism) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }

    final ClusterConfig config = ClusterConfig.fromWorkers(workersNode.workers());
    LOG.info("Pushing configuration {}", config);
    this.remoteRuntime = new RemoteRuntime(curator, new KryoSerializer(), config);
  }

  @Override
  public void close() {
    try {
      remoteRuntime.close();
      curator.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      workers.forEach(WorkerApplication::close);
      zooKeeperApplication.shutdown();
    }
  }

  @Override
  public Flame run(Graph g) {
    return remoteRuntime.run(g);
  }

  public String zkString() {
    return zkString;
  }

  private Set<Integer> freePorts(int n) throws IOException {
    final Set<ServerSocket> sockets = new HashSet<>();
    final Set<Integer> ports = new HashSet<>();
    try {
      for (int i = 0; i < n; ++i) {
        final ServerSocket socket = new ServerSocket(0);
        ports.add(socket.getLocalPort());
        sockets.add(socket);
      }
    } finally {
      for (ServerSocket socket : sockets) {
        socket.close();
      }
    }
    return ports;
  }
}
