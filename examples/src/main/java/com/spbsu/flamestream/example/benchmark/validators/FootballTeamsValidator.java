package com.spbsu.flamestream.example.benchmark.validators;

import com.spbsu.flamestream.example.benchmark.BenchValidator;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: Artem
 * Date: 03.01.2018
 */
@SuppressWarnings("unused")
public class FootballTeamsValidator implements BenchValidator<WordIndexAdd> {
  private final Map<String, TLongSet> expectedPositions = new ConcurrentHashMap<>();

  public FootballTeamsValidator() {
    expectedPositions.put("список", new TLongHashSet(new long[] {378658612051972096L, 5010205107688574976L, 1021217604094660608L, 297855501718261760L, 1047457449189052416L, 251379145025523712L, 249818937769857024L, 253682631542050816L, 775100723514839040L, 221447142197170176L, 253702417571385344L, 849451901973958656L, 1043799378626613248L, 6896515161886035968L, 399266757594845184L, 775452566151499776L, 250799703825846272L, 5485521785425235968L, 253934409641627648L, 786489464057761792L, 250799703849963520L, 221447151967801344L, 4763837537658605568L, 249848627253481472L, 1118870729507475456L, 253682630474600448L, 249196617236746240L, 1043799378368663552L, 5045707238562336768L, 992655593195966464L, 279070346989015040L, 245295553413386240L, 1105354432805081088L, 597452628598722560L, 635986112979931136L, 253977290634956800L, 283593735959220224L, 635986113054380032L, 688925398989279232L, 246477522131226624L, 5042349330058448896L, 249848627259772928L, 720617723037487104L, 751030213709664256L, 2235894278629888000L, 992655593249443840L, 1122374873177395200L, 86705293699846144L, 250624882385096704L, 56763388951269376L, 104680104640122880L}));
    expectedPositions.put("петербург", new TLongHashSet(new long[] {5010205107497734144L, 1021217604052717568L, 5045707238428119040L}));
    expectedPositions.put("россия", new TLongHashSet(new long[] {1021217604056911872L, 1119610700565581824L, 1119610700583407616L, 1119610700569776128L, 250624882206838784L, 86705289377615872L, 250624881170845696L, 86705289610399744L, 252820604699283456L, 251981679832338432L, 253682625756008448L}));
  }

  @Override
  public void accept(WordIndexAdd wordIndexAdd) {
    expectedPositions.computeIfPresent(wordIndexAdd.word(), (word, set) -> {
      Arrays.stream(wordIndexAdd.positions()).forEach(set::remove);
      return set;
    });
  }

  @Override
  public int inputLimit() {
    return 300; //more than in file
  }

  @Override
  public int expectedOutputSize() {
    return 65813;
  }

  @Override
  public void stop() {
    expectedPositions.forEach((word, set) -> {
      if (!set.isEmpty()) {
        throw new IllegalStateException("The following positions for word \"" + word + "\" were not received: " + set);
      }
    });
  }
}
