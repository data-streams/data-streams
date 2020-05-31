package com.spbsu.flamestream.runtime.invalidation;

import akka.actor.ActorSystem;
import akka.serialization.Serialization;
import akka.serialization.SerializationExtension;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.state.RedisStateStorage;
import com.spbsu.flamestream.runtime.state.RocksDBStateStorage;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DoubleGroupingTest extends FlameStreamSuite {
  private static final int WINDOW = 2;
  private static final HashFunction HASH_FUNCTION = HashFunction.uniformHash(HashFunction.bucketedHash(
          HashFunction.objectHash(Integer.class),
          20
  ));
  private static final Equalz EQUALZ = Equalz.hashEqualz(HASH_FUNCTION);

  private Graph graph() {
    final Grouping<Integer> firstGroup = new Grouping<>(HASH_FUNCTION, EQUALZ, WINDOW, Integer.class);
    final FlameMap<List<Integer>, Integer> firstHash =
            new FlameMap.Builder<>((List<Integer> longs2) -> Stream.of(longs2.hashCode()), List.class).build();
    final Grouping<Integer> secondGroup = new Grouping<>(HASH_FUNCTION, EQUALZ, WINDOW, Integer.class);
    final FlameMap<List<Integer>, Integer> secondHash =
            new FlameMap.Builder<>((List<Integer> longs1) -> Stream.of(longs1.hashCode()), List.class).build();
    final Grouping<Integer> thirdGroup = new Grouping<>(HASH_FUNCTION, EQUALZ, WINDOW, Integer.class);
    final FlameMap<List<Integer>, Integer> thirdHash =
            new FlameMap.Builder<>((List<Integer> longs) -> Stream.of(longs.hashCode()), List.class).build();


    final Source source = new Source();
    final Sink sink = new Sink();

    return new Graph.Builder()
            .link(source, firstGroup)
            .link(firstGroup, firstHash)
            .link(firstHash, secondGroup)
            .link(secondGroup, secondHash)
            .link(secondHash, thirdGroup)
            .link(thirdGroup, thirdHash)
            .link(thirdHash, sink)
            .build(source, sink);
  }

  @Test(invocationCount = 10)
  public void singleWorkerTest() throws InterruptedException {
    try (LocalRuntime runtime = new LocalRuntime.Builder().parallelism(1).build()) {
      doubleGroupingTest(runtime, 10000, false);
    }
  }

  @Test(invocationCount = 10)
  public void multipleWorkersTest() throws InterruptedException {
    try (LocalRuntime runtime = new LocalRuntime.Builder().parallelism(4).build()) {
      doubleGroupingTest(runtime, 10000, false);
    }
  }

  @Test(invocationCount = 10)
  public void distributedAckerMultipleWorkersTest() throws InterruptedException {
    try (
            LocalRuntime runtime = new LocalRuntime.Builder()
                    .parallelism(4)
                    .acking(SystemConfig.Acking.DISTRIBUTED)
                    .build()
    ) {
      doubleGroupingTest(runtime, 10000, false);
    }
  }

  @Test
  public void singleWorkerBlinkTest() throws InterruptedException {
    try (LocalRuntime runtime = new LocalRuntime.Builder().parallelism(1).withBlink().build()) {
      doubleGroupingTest(runtime, 20_000, true);
    }
  }

  @Test
  public void multipleWorkersBlinkTest() throws InterruptedException {
    try (LocalRuntime runtime = new LocalRuntime.Builder().parallelism(4).withBlink().build()) {
      doubleGroupingTest(runtime, 20_000, true);
    }
  }

  @Test
  public void multipleWorkersDistributedAckerBlinkTest() throws InterruptedException {
    try (
            LocalRuntime runtime = new LocalRuntime.Builder()
                    .parallelism(4)
                    .withBlink()
                    .acking(SystemConfig.Acking.DISTRIBUTED)
                    .build()
    ) {
      doubleGroupingTest(runtime, 20_000, true);
    }
  }

  @Test(enabled = false)
  public void redisMultipleWorkersBlinkTest() throws InterruptedException {
    final ActorSystem system = ActorSystem.create("local-runtime", ConfigFactory.load("local"));
    final Serialization serialization = SerializationExtension.get(system);
    final RedisStateStorage redisStateStorage = new RedisStateStorage("localhost", 6379, serialization);
    redisStateStorage.clear();

    try (final LocalRuntime runtime = new LocalRuntime.Builder().parallelism(4)
            .systemConfig(new SystemConfig.Builder().millisBetweenCommits(50))
            .withSystem(system)
            .withStateStorage(redisStateStorage)
            .withBlink()
            .build()) {
      doubleGroupingTest(runtime, 20_000, true);
    }
  }

  @Test
  public void rocksDBMultipleWorkersBlinkTest() throws InterruptedException, IOException {
    final ActorSystem system = ActorSystem.create("local-runtime", ConfigFactory.load("local"));
    final Serialization serialization = SerializationExtension.get(system);
    final String pathToDb = "./rocksdb";
    final RocksDBStateStorage rocksDBStateStorage = new RocksDBStateStorage(pathToDb, serialization);

    try (final LocalRuntime runtime = new LocalRuntime.Builder().parallelism(4)
            .withSystem(system)
            .withStateStorage(rocksDBStateStorage)
            .withBlink()
            .build()) {
      doubleGroupingTest(runtime, 20_000, true);
    }

    rocksDBStateStorage.close();
    FileUtils.deleteDirectory(new File(pathToDb));
  }

  private void doubleGroupingTest(LocalRuntime runtime, long inputSize, boolean backpressure) throws
                                                                                              InterruptedException {
    try (final FlameRuntime.Flame flame = runtime.run(graph())) {
      final List<Integer> source = new Random()
              .ints(inputSize)
              .boxed().collect(Collectors.toList());

      final List<Integer> expected = expected(source);
      final AwaitResultConsumer<Integer> consumer = new AwaitResultConsumer<>(expected.size());
      flame.attachRear("doubleGroupingRear", new AkkaRearType<>(runtime.system(), Integer.class))
              .forEach(r -> r.addListener(consumer));

      final List<AkkaFront.FrontHandle<Integer>> handles = flame.attachFront(
              "doubleGroupingFront",
              new AkkaFrontType<Integer>(runtime.system(), backpressure)
      ).collect(Collectors.toList());
      final AkkaFront.FrontHandle<Integer> sink = handles.get(0);

      for (int i = 1; i < handles.size(); i++) {
        handles.get(i).unregister();
      }

      source.forEach(sink);
      sink.unregister();

      consumer.await(10, TimeUnit.MINUTES);
      Assert.assertEquals(consumer.result().collect(Collectors.toSet()), new HashSet<>(expected));
    }
  }

  private List<Integer> expected(List<Integer> source) {
    return semanticGrouping(
            semanticGrouping(
                    semanticGrouping(
                            source
                    ).stream().map(List::hashCode).collect(Collectors.toList())
            ).stream().map(List::hashCode).collect(Collectors.toList())
    ).stream().map(List::hashCode).collect(Collectors.toList());
  }

  private static <T> List<List<T>> semanticGrouping(List<T> toBeGrouped) {
    final List<List<T>> result = new ArrayList<>();

    final Map<Wrapper<T>, List<T>> groups = new HashMap<>();
    for (T item : toBeGrouped) {
      final List<T> currentGroup = groups.getOrDefault(new Wrapper<>(item), new ArrayList<>());
      currentGroup.add(item);
      result.add(new ArrayList<>(currentGroup.subList(
              Math.max(0, currentGroup.size() - WINDOW),
              currentGroup.size()
      )));
      groups.put(new Wrapper<>(item), currentGroup);
    }

    return result;
  }

  private static class Wrapper<T> {
    private final T value;

    private Wrapper(T value) {
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Wrapper<?> wrapper = (Wrapper<?>) o;
      return EQUALZ.test(
              new PayloadDataItem(new Meta(GlobalTime.MAX), value),
              new PayloadDataItem(new Meta(GlobalTime.MAX), wrapper.value)
      );
    }

    @Override
    public int hashCode() {
      return HASH_FUNCTION.hash(new PayloadDataItem(new Meta(GlobalTime.MAX), value));
    }
  }
}
