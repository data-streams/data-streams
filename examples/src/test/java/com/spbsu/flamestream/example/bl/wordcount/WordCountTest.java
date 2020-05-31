package com.spbsu.flamestream.example.bl.wordcount;

import com.spbsu.flamestream.example.bl.wordcount.model.WordCounter;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * User: Artem
 * Date: 19.12.2017
 */
public class WordCountTest extends FlameAkkaSuite {
  @Test(invocationCount = 10)
  public void localEnvironmentTest() throws InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime.Builder()
            .systemConfig(new SystemConfig.Builder().maxElementsInGraph(2).millisBetweenCommits(500))
            .build()) {
      test(runtime, 2000);
    }
  }

  @Test
  public void localEnvironmentBlinkTest() throws InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime.Builder()
            .systemConfig(new SystemConfig.Builder().maxElementsInGraph(2).millisBetweenCommits(500))
            .withBlink()
            .build()) {
      test(runtime, 10000);
    }
  }

  private void test(LocalRuntime runtime, int streamSize) throws InterruptedException {
    try (final FlameRuntime.Flame flame = runtime.run(new WordCountGraph().get())) {
      final int lineSize = 50;
      final Queue<String> input = Stream.generate(() -> {
        final String[] words = {"repka", "dedka", "babka", "zhuchka", "vnuchka"};
        return new Random().ints(lineSize, 0, words.length).mapToObj(i -> words[i])
                .collect(joining(" "));
      }).limit(streamSize).collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
      final Pattern pattern = Pattern.compile("\\s");
      final Map<String, Integer> expected = input.stream()
              .map(pattern::split)
              .flatMap(Arrays::stream)
              .collect(toMap(Function.identity(), o -> 1, Integer::sum));

      final AwaitResultConsumer<WordCounter> awaitConsumer = new AwaitResultConsumer<>(
              lineSize * streamSize
      );
      flame.attachRear("wordCountRear", new AkkaRearType<>(runtime.system(), WordCounter.class))
              .forEach(r -> r.addListener(awaitConsumer));
      final List<AkkaFront.FrontHandle<String>> handles = flame
              .attachFront("wordCountFront", new AkkaFrontType<String>(runtime.system()))
              .collect(Collectors.toList());
      applyDataToAllHandlesAsync(input, handles);
      awaitConsumer.await(5, TimeUnit.MINUTES);

      final Map<String, Integer> actual = new HashMap<>();
      awaitConsumer.result().forEach(o -> {
        final WordCounter wordContainer = o;
        actual.putIfAbsent(wordContainer.word(), 0);
        actual.computeIfPresent(wordContainer.word(), (uid, old) -> Math.max(wordContainer.count(), old));
      });

      Assert.assertEquals(actual, expected);
    }
  }
}
