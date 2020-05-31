package com.spbsu.flamestream.example.bl.topwordcount;

import com.spbsu.flamestream.example.bl.topwordcount.model.WordsTop;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Comparator;
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
public class TopWordCountGraphTest extends FlameAkkaSuite {

  @DataProvider
  public static Object[][] dataProvider() {
    return new Object[][]{
            {SystemConfig.Acking.CENTRALIZED},
            {SystemConfig.Acking.DISTRIBUTED}
    };
  }

  @Test(dataProvider = "dataProvider", invocationCount = 10)
  public void topWordCountTest(SystemConfig.Acking acking) throws InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime.Builder()
            .systemConfig(new SystemConfig.Builder().maxElementsInGraph(2).millisBetweenCommits(500))
            .acking(acking)
            .build()) {
      try (final FlameRuntime.Flame flame = runtime.run(new TopWordCountGraph().get())) {
        final int lineSize = 20;
        final int streamSize = 10;
        final Queue<String> input = Stream.generate(() -> {
          final String[] words = {"repka", "dedka", "babka", "zhuchka", "vnuchka"};
          return new Random().ints(lineSize, 0, words.length).mapToObj(i -> words[i])
                  .collect(joining(" "));
        }).limit(streamSize).collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        final Pattern pattern = Pattern.compile("\\s");
        final Map<String, Integer> wordCounts = input.stream()
                .map(pattern::split)
                .flatMap(Arrays::stream)
                .collect(toMap(Function.identity(), o -> 1, Integer::sum));

        final AwaitResultConsumer<WordsTop> awaitConsumer = new AwaitResultConsumer<>(
                lineSize * streamSize
        );
        flame.attachRear("wordCountRear", new AkkaRearType<>(runtime.system(), WordsTop.class))
                .forEach(r -> r.addListener(awaitConsumer));
        final List<AkkaFront.FrontHandle<String>> handles = flame
                .attachFront("wordCountFront", new AkkaFrontType<String>(runtime.system()))
                .collect(Collectors.toList());
        applyDataToAllHandlesAsync(input, handles);
        awaitConsumer.await(200, TimeUnit.SECONDS);

        //noinspection ConstantConditions
        final WordsTop actualWordsTop = awaitConsumer.result().skip(lineSize * streamSize - 1).findFirst().get();
        final Stream<Map.Entry<String, Integer>> entryStream = wordCounts.entrySet()
                .stream()
                .filter(entry -> actualWordsTop.wordCounters().containsKey(entry.getKey()));
        Assert.assertEquals(
                actualWordsTop.wordCounters(),
                entryStream
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
        Assert.assertEquals(
                dewordifiedTop(actualWordsTop.wordCounters()),
                dewordifiedTop(wordCounts).stream().limit(2).collect(Collectors.toList())
        );
        Assert.assertEquals(actualWordsTop.wordCounters().size(), 2);
      }
    }
  }

  private List<Integer> dewordifiedTop(Map<String, Integer> wordCounts) {
    return wordCounts.values().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
  }
}
