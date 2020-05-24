package com.spbsu.flamestream.example.graph_search;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Scanner;

public class GraphDataset {
  public static void main(String[] args) throws Exception {
    try (
            final FileInputStream fileInputStream = new FileInputStream("/Users/nikitasokolov/Downloads/links-anon.txt");
            final FileOutputStream fileOutputStream = new FileOutputStream("/Users/nikitasokolov/Downloads/links-anon.bin");
            final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            final DataOutputStream output = new DataOutputStream(bufferedOutputStream)
    ) {
      final Scanner scanner = new Scanner(new BufferedInputStream(fileInputStream));
      while (scanner.hasNext()) {
        output.writeInt(scanner.nextInt());
      }
    }
  }
}
