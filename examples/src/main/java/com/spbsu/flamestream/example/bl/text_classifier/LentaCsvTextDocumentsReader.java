package com.spbsu.flamestream.example.bl.text_classifier;

import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LentaCsvTextDocumentsReader {
  private static final Pattern WORD_PATTERN = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);

  public static Stream<TextDocument> documents(InputStream inputStream) throws IOException {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
            new CSVParser(new BufferedReader(new InputStreamReader(
                    inputStream,
                    StandardCharsets.UTF_8
            )), CSVFormat.DEFAULT.withFirstRecordAsHeader()).iterator(),
            Spliterator.IMMUTABLE
    ), false).map(r -> document(r.get(4), r.get(1), (int) r.getRecordNumber(), r.get(4)));
  }

  public static Stream<TextDocument> bootstrap(InputStream inputStream) throws IOException {
    List<CSVRecord> sample = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
            new CSVParser(new BufferedReader(new InputStreamReader(
                    inputStream,
                    StandardCharsets.UTF_8
            )), CSVFormat.DEFAULT.withFirstRecordAsHeader()).iterator(),
            Spliterator.IMMUTABLE
    ), false).collect(Collectors.toList());
    final Random random = new Random();
    AtomicInteger counter = new AtomicInteger(0);
    return Stream.generate(() -> sample.get(random.nextInt(sample.size()))).limit(20000)
            .map(r -> document(r.get(4), r.get(1), counter.incrementAndGet(), r.get(4)));
  }

  public static Stream<TextDocument> groupedBootstrap(InputStream inputStream) throws IOException {
    List<CSVRecord> sample = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
            new CSVParser(new BufferedReader(new InputStreamReader(
                    inputStream,
                    StandardCharsets.UTF_8
            )), CSVFormat.DEFAULT.withFirstRecordAsHeader()).iterator(),
            Spliterator.IMMUTABLE
    ), false).limit(10000).collect(Collectors.toList());
    Map<String, List<CSVRecord>> byTopic = sample.stream()
            .collect(Collectors.groupingBy(csvRecord -> csvRecord.get(3)));
    final Random random = new Random();
    AtomicInteger counter = new AtomicInteger(0);
    return Stream.generate(() -> sample.get(random.nextInt(sample.size())).get(3)).limit(300)
            .flatMap(topic -> Stream.generate(() -> {
              final List<CSVRecord> csvRecords = byTopic.get(topic);
              return csvRecords.get(random.nextInt(csvRecords.size()));
            }).limit(100))
            .map(r -> document(r.get(4), r.get(1), counter.incrementAndGet(), r.get(4)));
  }

  public static TextDocument document(String name, String recordText, int number, String label) {
    StringJoiner text = new StringJoiner(" ");
    Matcher matcher = WORD_PATTERN.matcher(recordText);
    while (matcher.find()) {
      text.add(matcher.group());
    }
    return new TextDocument(
            name, // url order
            text.toString().toLowerCase(),
            String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
            number,
            label // label
    );
  }
}
