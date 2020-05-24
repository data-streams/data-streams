package com.spbsu.flamestream.example.graph_search;

import java.io.File;

public class Edges {
  private static final int[] heads;
  static {
    final File shardFile = new File(System.getenv("EDGES"));
    final String[] shardOfAll = shardFile.getName().split(".", 2);
    final int shardIndex = Integer.parseInt(shardOfAll[0]), shards = Integer.parseInt(shardOfAll[1]);
    final File root = shardFile.getParentFile();
    if (!shardFile.exists()) {
    }
    heads = new int[0];
  }
}
