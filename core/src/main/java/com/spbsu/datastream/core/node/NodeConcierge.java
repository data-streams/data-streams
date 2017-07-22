package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.front.FrontActor;
import com.spbsu.datastream.core.tick.TickConcierge;
import com.spbsu.datastream.core.tick.TickInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbImpl;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

public final class NodeConcierge extends LoggingActor {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final ZooKeeper zooKeeper;
  private final int id;

  private DB db;
  private ActorRef dnsRouter;
  private ActorRef tickRouter;

  private ActorRef front;

  private NodeConcierge(int id, ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.id = id;
  }

  public static Props props(int id, ZooKeeper zooKeeper) {
    return Props.create(NodeConcierge.class, id, zooKeeper);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();

    this.db = new DbImpl(new Options().createIfMissing(true), new File("./leveldb/" + this.id));

    this.tickRouter = this.context().actorOf(TickRouter.props(), "tickRouter");

    final Map<Integer, InetSocketAddress> dns = this.fetchDNS();
    this.LOG().info("DNS fetched: {}", dns);
    this.dnsRouter = this.context().actorOf(DNSRouter.props(dns, this.tickRouter, this.id), "dns");

    final Set<Integer> fronts = this.fetchFronts();
    this.LOG().info("Fronts fetched: {}", fronts);

    if (fronts.contains(this.id)) {
      this.front = this.context().actorOf(FrontActor.props(this.dnsRouter, this.id), "front");
    }

    this.context().actorOf(TickWatcher.props(this.zooKeeper, this.self()), "tickWatcher");
  }

  @Override
  public void postStop() throws Exception {
    super.postStop();

    this.db.close();
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder().match(TickInfo.class, this::onNewTick).build();
  }

  private void onNewTick(TickInfo tickInfo) {
    // FIXME: 7/6/17 this two events are not ordered
    final ActorRef tickConcierge = this.context().actorOf(TickConcierge.props(tickInfo, this.db, this.id, this.dnsRouter), String.valueOf(tickInfo.startTs()));
    this.tickRouter.tell(new TickRouter.RegisterTick(tickInfo.startTs(), tickConcierge), this.self());

    if (this.front != null) {
      this.front.tell(tickInfo, this.self());
    }
  }

  private Map<Integer, InetSocketAddress> fetchDNS() throws IOException, KeeperException, InterruptedException {
    final String path = "/dns";
    final byte[] data = this.zooKeeper.getData(path, false, new Stat());
    return NodeConcierge.MAPPER.readValue(data, new TypeReference<Map<Integer, InetSocketAddress>>() {
    });
  }

  private Set<Integer> fetchFronts() throws KeeperException, InterruptedException, IOException {
    final String path = "/fronts";
    final byte[] data = this.zooKeeper.getData(path, false, new Stat());
    return NodeConcierge.MAPPER.readValue(data, new TypeReference<Set<Integer>>() {
    });
  }
}