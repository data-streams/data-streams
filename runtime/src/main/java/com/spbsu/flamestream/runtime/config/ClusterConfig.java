package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorPath;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterConfig {
  private final Map<String, ActorPath> paths;
  private final String masterLocation;

  public ClusterConfig(Map<String, ActorPath> paths, String masterLocation) {
    this.paths = paths;
    this.masterLocation = masterLocation;
  }

  public LinkedHashMap<String, ActorPath> paths() {
    return new LinkedHashMap<>(paths);
  }

  public String masterLocation() {
    return masterLocation;
  }

  public ActorPath masterPath() {
    return paths.get(masterLocation);
  }

  public ClusterConfig withChildPath(String childPath) {
    final Map<String, ActorPath> newPaths = new LinkedHashMap<>();
    paths.forEach((s, path) -> newPaths.put(s, path.child(childPath)));
    return new ClusterConfig(newPaths, masterLocation);
  }

  @Override
  public String toString() {
    return "ClusterConfig{" +
            "paths=" + paths +
            ", masterLocation='" + masterLocation + '\'' +
            '}';
  }

  public static ClusterConfig fromWorkers(List<ZookeeperWorkersNode.Worker> workers) {
    final Map<String, ActorPath> paths = workers.stream()
            .collect(Collectors.toMap(
                    ZookeeperWorkersNode.Worker::id,
                    ZookeeperWorkersNode.Worker::actorPath,
                    (k, v) -> {throw new RuntimeException();},
                    LinkedHashMap::new
            ));
    final String masterLocation = workers.get(0).id;

    return new ClusterConfig(paths, masterLocation);
  }
}
