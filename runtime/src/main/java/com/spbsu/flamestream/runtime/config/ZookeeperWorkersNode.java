package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.flamestream.runtime.serialization.FlameSerializer;
import com.spbsu.flamestream.runtime.serialization.JacksonSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.zookeeper.CreateMode;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ZookeeperWorkersNode {
  private final CuratorFramework curator;
  private final String path;
  private final FlameSerializer jacksonSerializer = new JacksonSerializer();
  private final PathChildrenCache pathChildrenCache;

  static public class Worker {
    public final String id;

    @JsonProperty
    public String id() { return id;}

    final ActorPath actorPath;

    @JsonProperty
    ActorPath actorPath() { return actorPath; }

    @JsonCreator
    Worker(@JsonProperty("id") String id, @JsonProperty("actorPath") ActorPath actorPath) {
      this.id = id;
      this.actorPath = actorPath;
    }
  }

  public ZookeeperWorkersNode(CuratorFramework curator, String path) {
    this.curator = curator;
    this.path = path;
    this.pathChildrenCache = new PathChildrenCache(curator, path, true);
    try {
      pathChildrenCache.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void create(String id, ActorPath actorPath) {
    try {
      curator.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
              .forPath(path + "/_", jacksonSerializer.serialize(new Worker(id, actorPath)));
      pathChildrenCache.rebuild();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<Worker> workers() {
    try {
      pathChildrenCache.rebuild();
      return pathChildrenCache.getCurrentData()
              .stream()
              .map(worker -> jacksonSerializer.deserialize(worker.getData(), Worker.class))
              .collect(Collectors.toList());
    } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
      return Collections.emptyList();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
