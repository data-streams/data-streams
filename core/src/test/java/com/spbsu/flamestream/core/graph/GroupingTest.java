package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import org.jetbrains.annotations.NotNull;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class GroupingTest extends FlameStreamSuite {
  private static Set<List<String>> groupMe(Stream<DataItem> input, int window) {
    return groupMe(input, new Grouping<>(
            HashFunction.constantHash(1),
            (t, t2) -> true,
            window,
            String.class
    ));
  }

  @NotNull
  private static Set<List<String>> groupMe(Stream<DataItem> input, Grouping<String> grouping) {
    final Map<DataItem, InvalidatingBucket> state = new HashMap<>();

    final List<DataItem> toBeOutput = new ArrayList<>();

    input.map(di -> new DataItemForTest(di, grouping.hash(), grouping.equalz()))
            .flatMap(di -> {
              state.putIfAbsent(di, new ArrayInvalidatingBucket());
              return grouping.operation(1).apply(di, state.get(di));
            })
            .forEach(dataItem -> {
              if (dataItem.meta().isTombstone()) {
                final boolean anIf = toBeOutput.removeIf(i -> i.meta().isInvalidedBy(dataItem.meta()));
                if (!anIf) {
                  throw new IllegalStateException("Releasing tombstone that doesn't invalidate anything");
                }
              } else {
                toBeOutput.add(dataItem);
              }
            });

    return toBeOutput.stream().map(dataItem -> dataItem.payload(List.class))
            .map(element -> (List<String>) element)
            .collect(Collectors.toSet());
  }

  @Test
  public void withoutReordering() {
    final DataItem x1 = new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "v1");
    final DataItem x2 = new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "v2");
    final DataItem x3 = new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "v3");

    final Set<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x1, x2, x3), 2);
    final Set<List<String>> expectedResult = new HashSet<>();

    final List<String> y1 = Collections.singletonList(x1.payload(String.class));
    final List<String> y2 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));
    final List<String> y3 = Arrays.asList(x2.payload(String.class), x3.payload(String.class));

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void groupWithSuccessor() {
    final Meta x5Global = new Meta(new GlobalTime(2, EdgeId.MIN));
    final Meta x0Global = new Meta(new GlobalTime(1, EdgeId.MIN));

    final DataItem x5 = new PayloadDataItem(x5Global, "v5");
    final DataItem x5State = new PayloadDataItem(new Meta(x5.meta(), 2, 0), "v5State");
    final DataItem x0 = new PayloadDataItem(x0Global, "x0");
    final DataItem x0State = new PayloadDataItem(new Meta(x0.meta(), 2, 0), "x0State");
    final DataItem theState = new PayloadDataItem(new Meta(x5State.meta(), 4, 0), "theState");

    final Set<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x5, x5State, x0, x0State, theState), 2);
    final Set<List<String>> expectedResult = new HashSet<>();

    final List<String> y3 = Collections.singletonList(x0.payload(String.class));
    final List<String> y5 = Arrays.asList(x0.payload(String.class), x0State.payload(String.class));
    final List<String> y6 = Arrays.asList(x0State.payload(String.class), x5.payload(String.class));
    final List<String> y7 = Arrays.asList(x5.payload(String.class), x5State.payload(String.class));
    final List<String> y8 = Arrays.asList(x5State.payload(String.class), theState.payload(String.class));

    expectedResult.add(y3);
    expectedResult.add(y5);
    expectedResult.add(y6);
    expectedResult.add(y7);
    expectedResult.add(y8);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void cycleSimulation() {
    final Meta x1Meta = new Meta(new GlobalTime(1, EdgeId.MIN));

    final DataItem x1 = new PayloadDataItem(x1Meta, "v1");
    final DataItem x2 = new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "v2");
    final DataItem x1Prime = new PayloadDataItem(new Meta(x1Meta, 2, 0), "state");

    final Set<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x1, x2, x1Prime), 2);
    final Set<List<String>> expectedResult = new HashSet<>();

    final List<String> y1 = Collections.singletonList(x1.payload(String.class));
    final List<String> y3 = Arrays.asList(x1.payload(String.class), x1Prime.payload(String.class));
    final List<String> y4 = Arrays.asList(x1Prime.payload(String.class), x2.payload(String.class));

    expectedResult.add(y1);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void headReordering() {
    final DataItem x2 = new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "v2");
    final DataItem x1 = new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "v1");
    final DataItem x3 = new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "v3");

    final Set<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x2, x1, x3), 2);
    final Set<List<String>> expectedResult = new HashSet<>();

    final List<String> y2 = Collections.singletonList(x1.payload(String.class));
    final List<String> y3 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));
    final List<String> y4 = Arrays.asList(x2.payload(String.class), x3.payload(String.class));

    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void tailReordering() {
    final DataItem x1 = new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "v1");
    final DataItem x3 = new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "v3");
    final DataItem x2 = new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "v2");

    final Set<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x1, x3, x2), 2);
    final Set<List<String>> expectedResult = new HashSet<>();

    final List<String> y1 = Collections.singletonList(x1.payload(String.class));
    final List<String> y3 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));
    final List<String> y4 = Arrays.asList(x2.payload(String.class), x3.payload(String.class));

    expectedResult.add(y1);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void reverseReordering() {
    final DataItem x3 = new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "v3");
    final DataItem x2 = new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "v2");
    final DataItem x1 = new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "v1");

    final Set<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x3, x2, x1), 2);
    final Set<List<String>> expectedResult = new HashSet<>();

    final List<String> y3 = Arrays.asList(x2.payload(String.class), x3.payload(String.class));
    final List<String> y4 = Collections.singletonList(x1.payload(String.class));
    final List<String> y5 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));

    expectedResult.add(y3);
    expectedResult.add(y4);
    expectedResult.add(y5);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void reorderingWithInvalidating() {
    final DataItem x1 = new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "v1");
    final DataItem x2 = new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "v2");

    final Meta x3Meta = new Meta(new GlobalTime(3, EdgeId.MIN));
    final DataItem x3 = new PayloadDataItem(new Meta(x3Meta, 1, false), "v3");
    final DataItem x3Prime = new PayloadDataItem(new Meta(x3Meta, 1, true), "v3");

    final Set<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x1, x2, x3, x3Prime), 2);
    final Set<List<String>> expectedResult = new HashSet<>();

    final List<String> y1 = Collections.singletonList(x1.payload(String.class));
    final List<String> y2 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));

    expectedResult.add(y1);
    expectedResult.add(y2);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void shuffleReordering() {
    final List<DataItem> input = IntStream.range(0, 1000)
            .mapToObj(i -> new PayloadDataItem(new Meta(new GlobalTime(i, EdgeId.MIN)), "v" + i))
            .collect(Collectors.toList());

    final List<DataItem> shuffledInput = new ArrayList<>(input);
    Collections.shuffle(shuffledInput, new Random(2));

    final int window = 6;
    final Set<List<String>> mustHave = new HashSet<>(
            semanticGrouping(IntStream.range(0, 1000)
                    .mapToObj(i -> "v" + i)
                    .collect(Collectors.toList()), window)
    );

    Assert.assertEquals(
            GroupingTest.groupMe(shuffledInput.stream(), window),
            mustHave,
            "Result must contain expected elements"
    );
  }

  @Test
  public void undoPartialWindowsWithMaxSizeInOrder() {
    Assert.assertEquals(
            GroupingTest.groupMe(
                    Stream.of(
                            new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "1"),
                            new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "2"),
                            new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "3")
                    ),
                    new Grouping<>(new Grouping.Builder(
                            HashFunction.constantHash(1),
                            (t, t2) -> true,
                            Integer.MAX_VALUE,
                            String.class
                    ).undoPartialWindows(true))
            ),
            Collections.singleton(Arrays.asList("1", "2", "3"))
    );
  }

  @Test
  public void undoPartialWindowsWithMaxSizeOutOfOrder() {
    Assert.assertEquals(
            GroupingTest.groupMe(
                    Stream.of(
                            new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "2"),
                            new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "1"),
                            new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "3")
                    ),
                    new Grouping<>(new Grouping.Builder(
                            HashFunction.constantHash(1),
                            (t, t2) -> true,
                            Integer.MAX_VALUE,
                            String.class
                    ).undoPartialWindows(true))
            ),
            Collections.singleton(Arrays.asList("1", "2", "3"))
    );
  }

  @Test
  public void undoPartialWindowsWithSize2InOrder() {
    Assert.assertEquals(
            GroupingTest.groupMe(
                    Stream.of(
                            new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "1"),
                            new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "2"),
                            new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "3")
                    ),
                    new Grouping<>(new Grouping.Builder(
                            HashFunction.constantHash(1),
                            (t, t2) -> true,
                            2,
                            String.class
                    ).undoPartialWindows(true))
            ),
            Stream.of(Arrays.asList("1", "2"), Arrays.asList("2", "3")).collect(Collectors.toSet())
    );
  }

  @Test
  public void undoPartialWindowsWithSize2OutOfOrder() {
    Assert.assertEquals(
            GroupingTest.groupMe(
                    Stream.of(
                            new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "3"),
                            new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "2"),
                            new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "1")
                    ),
                    new Grouping<>(new Grouping.Builder(
                            HashFunction.constantHash(1),
                            (t, t2) -> true,
                            2,
                            String.class
                    ).undoPartialWindows(true))
            ),
            Stream.of(Arrays.asList("1", "2"), Arrays.asList("2", "3")).collect(Collectors.toSet())
    );
  }

  @Test
  public void undoPartialWindowsWithSize3InOrder() {
    Assert.assertEquals(
            GroupingTest.groupMe(
                    Stream.of(
                            new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "1"),
                            new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "2"),
                            new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "3"),
                            new PayloadDataItem(new Meta(new GlobalTime(4, EdgeId.MIN)), "4")
                    ),
                    new Grouping<>(new Grouping.Builder(
                            HashFunction.constantHash(1),
                            (t, t2) -> true,
                            3,
                            String.class
                    ).undoPartialWindows(true))
            ),
            Stream.of(Arrays.asList("1", "2", "3"), Arrays.asList("2", "3", "4")).collect(Collectors.toSet())
    );
  }

  @Test
  public void undoPartialWindowsWithSize3OutOfOrder() {
    Assert.assertEquals(
            GroupingTest.groupMe(
                    Stream.of(
                            new PayloadDataItem(new Meta(new GlobalTime(4, EdgeId.MIN)), "4"),
                            new PayloadDataItem(new Meta(new GlobalTime(3, EdgeId.MIN)), "3"),
                            new PayloadDataItem(new Meta(new GlobalTime(2, EdgeId.MIN)), "2"),
                            new PayloadDataItem(new Meta(new GlobalTime(1, EdgeId.MIN)), "1")
                    ),
                    new Grouping<>(new Grouping.Builder(
                            HashFunction.constantHash(1),
                            (t, t2) -> true,
                            3,
                            String.class
                    ).undoPartialWindows(true))
            ),
            Stream.of(Arrays.asList("1", "2", "3"), Arrays.asList("2", "3", "4")).collect(Collectors.toSet())
    );
  }

  private static <T> List<List<T>> semanticGrouping(List<T> toBeGrouped, int window) {
    final List<List<T>> result = new ArrayList<>();

    final List<T> group = new ArrayList<>();
    for (T item : toBeGrouped) {
      group.add(item);
      result.add(new ArrayList<>(group.subList(Math.max(0, group.size() - window), group.size())));
    }

    return result;
  }
}
