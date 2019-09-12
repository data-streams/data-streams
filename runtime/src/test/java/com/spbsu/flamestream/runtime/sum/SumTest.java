package com.spbsu.flamestream.runtime.sum;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.WorkerApplication;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import com.typesafe.config.ConfigFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class SumTest extends FlameAkkaSuite {

  private static Graph sumGraph() {
    final HashFunction identity = HashFunction.constantHash(1);

    //noinspection Convert2Lambda
    final Equalz predicate = new Equalz() {
      @Override
      public boolean test(DataItem dataItem, DataItem dataItem2) {
        return true;
      }
    };

    final Source source = new Source();
    final Grouping<Numb> grouping = new Grouping<>(identity, predicate, 2, Numb.class);
    final FlameMap<List<Numb>, List<Numb>> enricher = new FlameMap<>(
            new IdentityEnricher(),
            List.class
    );
    final FlameMap<List<Numb>, List<Numb>> junkFilter = new FlameMap<>(
            new WrongOrderingFilter(),
            List.class
    );
    final FlameMap<List<Numb>, Sum> reducer = new FlameMap<>(new Reduce(), List.class);
    final Sink sink = new Sink();

    return new Graph.Builder()
            .link(source, grouping)
            .link(grouping, enricher)
            .link(enricher, junkFilter)
            .link(junkFilter, reducer)
            .link(reducer, sink)
            .link(reducer, grouping)
            .build(source, sink);
  }

  @Test(invocationCount = 10)
  public void sumTest() throws InterruptedException {
    final int parallelism = DEFAULT_PARALLELISM;
    try (final LocalRuntime runtime = new LocalRuntime.Builder().parallelism(parallelism).build()) {
      try (final FlameRuntime.Flame flame = runtime.run(sumGraph())) {
        final List<AkkaFront.FrontHandle<LongNumb>> handles = flame.attachFront(
                "sumFront",
                new AkkaFrontType<LongNumb>(runtime.system())
        ).collect(Collectors.toList());
        final AtomicLong expected = new AtomicLong();
        final int inputSize = 5000;
        final List<List<LongNumb>> source = Stream.generate(() -> new Random()
                .ints(inputSize / parallelism, 0, 100)
                .peek(expected::addAndGet)
                .mapToObj(LongNumb::new)
                .collect(Collectors.toList())).limit(parallelism).collect(Collectors.toList());

        final AwaitResultConsumer<Sum> consumer = new AwaitResultConsumer<>(source.stream().mapToInt(List::size).sum());
        flame.attachRear("sumRear", new AkkaRearType<>(runtime.system(), Sum.class))
                .forEach(r -> r.addListener(consumer));
        IntStream.range(0, parallelism).forEach(i -> applyDataToHandleAsync(source.get(i).stream(), handles.get(i)));

        consumer.await(10, TimeUnit.MINUTES);
        final long actual = consumer.result()
                .mapToLong(Sum::value)
                .max()
                .orElseThrow(NoSuchElementException::new);
        Assert.assertEquals(actual, expected.get());
        Assert.assertEquals(consumer.result().count(), inputSize);
      }
    }
  }

  @Test(invocationCount = 10)
  public void totalOrderTest() throws InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime.Builder().build()) {
      try (final FlameRuntime.Flame flame = runtime.run(sumGraph())) {
        final List<LongNumb> source = new Random()
                .ints(1000)
                .mapToObj(LongNumb::new)
                .collect(Collectors.toList());
        final Set<Sum> expected = new HashSet<>();
        long currentSum = 0;
        for (LongNumb longNumb : source) {
          currentSum += longNumb.value();
          expected.add(new Sum(currentSum));
        }

        final List<AkkaFront.FrontHandle<LongNumb>> handles = flame.attachFront(
                "totalOrderFront",
                new AkkaFrontType<LongNumb>(runtime.system())
        ).collect(Collectors.toList());
        for (int i = 1; i < handles.size(); i++) {
          handles.get(i).unregister();
        }
        final AkkaFront.FrontHandle<LongNumb> sink = handles.get(0);

        final AwaitResultConsumer<Sum> consumer = new AwaitResultConsumer<>(source.size());
        flame.attachRear("totalOrderRear", new AkkaRearType<>(runtime.system(), Sum.class))
                .forEach(r -> r.addListener(consumer));
        source.forEach(sink);
        sink.unregister();

        consumer.await(10, TimeUnit.MINUTES);
        Assert.assertEquals(consumer.result().collect(Collectors.toSet()), expected);
      }
    }
  }

  @Test(invocationCount = 10)
  public void integrationTest() throws Exception {
    final ActorSystem system = ActorSystem.create("testStand", ConfigFactory.load("remote"));
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime(
            2,
            new SystemConfig.Builder().build()
    )) {
      try (final FlameRuntime.Flame flame = runtime.run(sumGraph())) {
        final List<LongNumb> source = new Random()
                .ints(1000)
                .mapToObj(LongNumb::new)
                .collect(Collectors.toList());
        final List<AkkaFront.FrontHandle<LongNumb>> handles = flame.attachFront(
                "totalOrderFront",
                new AkkaFrontType<LongNumb>(system, false)
        ).collect(Collectors.toList());
        for (int i = 1; i < handles.size(); i++) {
          handles.get(i).unregister();
        }
        final AkkaFront.FrontHandle<LongNumb> sink = handles.get(0);

        final AwaitResultConsumer<Sum> consumer = new AwaitResultConsumer<>(source.size());
        flame.attachRear("totalOrderRear", new AkkaRearType<>(system, Sum.class))
                .forEach(r -> r.addListener(consumer));

        final Set<Sum> expected = new HashSet<>();
        long currentSum = 0;
        for (LongNumb longNumb : source) {
          currentSum += longNumb.value();
          expected.add(new Sum(currentSum));
        }

        source.forEach(sink);
        sink.unregister();
        consumer.await(10, TimeUnit.MINUTES);
        Assert.assertEquals(consumer.result().collect(Collectors.toSet()), expected);
      }
    }
    Await.ready(system.terminate(), Duration.Inf());
  }
}
