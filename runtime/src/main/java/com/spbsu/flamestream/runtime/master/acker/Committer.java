package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.PatternsCS;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Commit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.GimmeLastCommit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.LastCommit;
import com.spbsu.flamestream.runtime.master.acker.api.commit.MinTimeUpdateListener;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepare;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepared;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Ready;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.akka.PingActor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;


public class Committer extends LoggingActor {
  private final Set<ActorRef> managers = new HashSet<>();

  private final int managersCount;
  private final int millisBetweenCommits;
  private final ActorRef registryHolder;
  private final ActorRef pingActor;

  private long minAmongTables;
  private GlobalTime lastPrepareTime = GlobalTime.MIN;
  private int committed;
  private boolean commitRuns = false;
  private long commitStartTime = -1;

  private Committer(int managersCount,
                    long defaultMinimalTime,
                    int millisBetweenCommits,
                    ActorRef registryHolder,
                    ActorRef acker) {
    this.managersCount = managersCount;
    this.minAmongTables = defaultMinimalTime;
    this.millisBetweenCommits = millisBetweenCommits;
    this.registryHolder = registryHolder;

    pingActor = context().actorOf(
            PingActor.props(self(), StartCommit.START),
            "acker-ping"
    );
    acker.tell(new MinTimeUpdateListener(self()), self());
  }

  public static Props props(
          int managersCount,
          SystemConfig systemConfig,
          ActorRef registryHolder,
          ActorRef acker
  ) {
    return Props.create(
            Committer.class,
            managersCount,
            systemConfig.defaultMinimalTime(),
            systemConfig.millisBetweenCommits(),
            registryHolder,
            acker
    ).withDispatcher("processing-dispatcher");
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    pingActor.tell(new PingActor.Start(TimeUnit.MILLISECONDS.toNanos(millisBetweenCommits)), self());
    minAmongTables = Math.max(((LastCommit) PatternsCS.ask(
            registryHolder,
            new GimmeLastCommit(),
            FlameConfig.config.smallTimeout()
    ).toCompletableFuture().get()).globalTime().time(), minAmongTables);
  }

  @Override
  public void postStop() {
    pingActor.tell(new PingActor.Stop(), self());
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Ready.class, ready -> {
              managers.add(sender());
              if (managers.size() == managersCount) {
                unstashAll();
                getContext().become(waiting());
              }
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive waiting() {
    return ReceiveBuilder.create()
            .match(StartCommit.class, __ -> commit(new GlobalTime(minAmongTables, EdgeId.MIN)))
            .match(MinTimeUpdate.class, minTimeUpdate -> minAmongTables = minTimeUpdate.minTime().time())
            .build();
  }

  private Receive committing() {
    return waiting().orElse(ReceiveBuilder.create()
            .match(Prepared.class, c -> {
              committed++;
              log().info("Manager '{}' has prepared", sender());
              if (committed == managersCount) {
                log().info("All managers have prepared, committing");
                final Commit commit = new Commit(lastPrepareTime);
                PatternsCS.ask(registryHolder, commit, FlameConfig.config.smallTimeout()).toCompletableFuture().get();
                committed = 0;
                commitRuns = false;
                managers.forEach(actorRef -> actorRef.tell(commit, self()));
                getContext().unbecome();
              }
            })
            .build());
  }

  private void commit(GlobalTime time) {
    if (!commitRuns && !time.equals(lastPrepareTime)) {
      log().info("Initiating commit for time '{}'", time);
      managers.forEach(m -> m.tell(new Prepare(time), self()));
      lastPrepareTime = time;
      commitRuns = true;
      commitStartTime = System.nanoTime();
      getContext().become(committing(), false);
    } else if (commitRuns && (System.nanoTime() - commitStartTime) > FlameConfig.config.bigTimeout()
            .duration()
            .toNanos()) {
      throw new RuntimeException("Commit timeout exceeded");
    }
  }

  private enum StartCommit {
    START
  }
}
