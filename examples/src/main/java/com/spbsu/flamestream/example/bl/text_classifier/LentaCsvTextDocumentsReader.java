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
  public static Stream<TextDocument> documents(InputStream inputStream) throws IOException {
    Iterator<CSVRecord> iter = new CSVParser(new BufferedReader(new InputStreamReader(
            inputStream,
            StandardCharsets.UTF_8
    )), CSVFormat.DEFAULT).iterator();
    // skip headers
    iter.next();

    Pattern wordPattern = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
    AtomicInteger counter = new AtomicInteger(0);
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
            iter,
            Spliterator.IMMUTABLE
    ), false).map(r -> {
      String recordText = r.get(2); // text order
      StringJoiner text = new StringJoiner(" ");
      Matcher matcher = wordPattern.matcher(recordText);
      while (matcher.find()) {
        text.add(matcher.group());
      }
      return new TextDocument(
              r.get(0), // url order
              text.toString().toLowerCase(),
              String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
              counter.incrementAndGet()
      );
    });
  }
}
