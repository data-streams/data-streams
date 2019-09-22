package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorPath;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterConfig {
  public final List<String> ids;
  private final Map<String, ActorPath> paths;

  public ClusterConfig(List<String> ids, Map<String, ActorPath> paths) {
    this.ids = ids;
    this.paths = paths;
  }

  public Map<String, ActorPath> paths() {
    return new HashMap<>(paths);
  }

  public ClusterConfig withChildPath(String childPath) {
    final Map<String, ActorPath> newPaths = new HashMap<>();
    paths.forEach((s, path) -> newPaths.put(s, path.child(childPath)));
    return new ClusterConfig(ids, newPaths);
  }

  @Override
  public String toString() {
    return "ClusterConfig{" +
            "paths=" + paths +
            '}';
  }

  public static ClusterConfig fromWorkers(List<ZookeeperWorkersNode.Worker> workers) {
    final Map<String, ActorPath> paths = workers.stream()
            .collect(Collectors.toMap(ZookeeperWorkersNode.Worker::id, ZookeeperWorkersNode.Worker::actorPath));

    return new ClusterConfig(
            workers.stream().map(ZookeeperWorkersNode.Worker::id).collect(Collectors.toList()),
            paths
    );
  }
}
