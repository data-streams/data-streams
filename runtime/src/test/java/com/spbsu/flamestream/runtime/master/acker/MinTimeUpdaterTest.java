package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestProbe;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.testng.Assert.*;

public class MinTimeUpdaterTest {

  @Test
  public void testOnShardMinTimeUpdate() {
    final ActorSystem system = ActorSystem.apply("test");
    final ActorRef shard1 = TestProbe.apply(system).ref(), shard2 = TestProbe.apply(system).ref();
    final ArrayList<ActorRef> shards = new ArrayList<>();
    shards.add(shard1);
    shards.add(shard2);
    final MinTimeUpdater minTimeUpdater = new MinTimeUpdater(shards, 0);
    final String id = "";
    final EdgeId edgeId = new EdgeId("", "");
    assertNull(minTimeUpdater.onShardMinTimeUpdate(
            shard2,
            new MinTimeUpdate(new GlobalTime(2, edgeId), new NodeTimes().updated(id, 1))
    ));
    final MinTimeUpdate minTimeUpdate = minTimeUpdater.onShardMinTimeUpdate(
            shard1,
            new MinTimeUpdate(new GlobalTime(1, edgeId), new NodeTimes().updated(id, 3))
    );
    assertNotNull(minTimeUpdate);
    assertEquals(minTimeUpdate.minTime(), new GlobalTime(1, edgeId));
  }
}
