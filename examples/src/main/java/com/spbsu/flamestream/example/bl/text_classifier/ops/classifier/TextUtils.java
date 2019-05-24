package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TextUtils {
  private static final Pattern PATTERN = Pattern.compile("\\b\\w\\w+\\b", Pattern.UNICODE_CHARACTER_CLASS);

  public static Stream<String> text2words(String text) {
    final Matcher matcher = PATTERN.matcher(text);
    final Iterable<String> iterable = () -> new Iterator<String>() {
      @Override
      public boolean hasNext() {
        return matcher.find();
      }

      @Override
      public String next() {
        return matcher.group(0);
      }
    };
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  public static String[] readTopics(String topicsPath) {
    try (final BufferedReader br = new BufferedReader(new InputStreamReader(
            new FileInputStream(topicsPath),
            StandardCharsets.UTF_8
    ))) {
      final double[] meta = Arrays
              .stream(br.readLine().split(" "))
              .mapToDouble(Double::parseDouble)
              .toArray();
      final int classes = (int) meta[0];
      String[] topics = new String[classes];
      for (int i = 0; i < classes; i++) {
        topics[i] = br.readLine();
      }

      return topics;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }
}
