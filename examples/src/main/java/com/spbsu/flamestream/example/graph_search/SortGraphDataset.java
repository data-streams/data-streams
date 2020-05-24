package com.spbsu.flamestream.example.graph_search;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Scanner;

public class SortGraphDataset {
  public static void main(String[] args) throws Exception {
    try (
            final FileInputStream fileInputStream = new FileInputStream("/Users/nikitasokolov/Downloads/links-anon-sorted.bin");
            final FileOutputStream fileOutputStream = new FileOutputStream("/Users/nikitasokolov/Downloads/vertices.bin");
            final DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(fileOutputStream))
    ) {
      final DataInputStream input = new DataInputStream(new BufferedInputStream(fileInputStream, 1_000_000));
      final BitSet vertices = new BitSet();
      while (true) {
        try {
          vertices.set(input.readInt());
        } catch (EOFException ignored) {
          break;
        }
      }
      int from = 0;
      while (true) {
        from = vertices.nextSetBit(from);
        if (from < 0)
          break;
        dataOutputStream.writeInt(from);
        from++;
      }
    }
  }
}
