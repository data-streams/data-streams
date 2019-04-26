package com.spbsu.flamestream.example.bl.text_classifier;

import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LentaCsvTextDocumentsReader {
  private static final Pattern WORD_PATTERN = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);

  public static Stream<TextDocument> documents(InputStream inputStream) throws IOException {
    Iterator<CSVRecord> iter = new CSVParser(new BufferedReader(new InputStreamReader(
            inputStream,
            StandardCharsets.UTF_8
    )), CSVFormat.DEFAULT).iterator();
    // skip headers
    iter.next();

    AtomicInteger counter = new AtomicInteger(0);
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
            iter,
            Spliterator.IMMUTABLE
    ), false).map(r -> document(r.get(0), r.get(2), counter.incrementAndGet()));
  }

  public static TextDocument document(String name, String recordText, int number) {
    StringJoiner text = new StringJoiner(" ");
    Matcher matcher = WORD_PATTERN.matcher(recordText);
    while (matcher.find()) {
      text.add(matcher.group());
    }
    return new TextDocument(
            name, // url order
            text.toString().toLowerCase(),
            String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
            number
    );
  }
}
