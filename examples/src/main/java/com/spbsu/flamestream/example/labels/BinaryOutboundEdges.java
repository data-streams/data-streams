package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.graph.SerializableFunction;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class BinaryOutboundEdges implements SerializableFunction<
        BreadthSearchGraph.VertexIdentifier,
        Stream<BreadthSearchGraph.VertexIdentifier>
        > {
  final int[] tails, tailHeadOffsets, heads;
  private int remainder;
  private final int divisor;

  public BinaryOutboundEdges(File tailFile, File headFile, int remainder, int divisor) throws IOException {
    this.remainder = remainder;
    this.divisor = divisor;
    int totalTails = 0, totalHeads = 0;
    try (final DataInputStream tailInput = newFileDataInputStream(tailFile)) {
      while (true) {
        final int tail;
        try {
          tail = tailInput.readInt();
        } catch (EOFException ignored) {
          break;
        }
        final int tailEdges = tailInput.readInt();
        if (tail % divisor == remainder) {
          totalTails++;
          totalHeads += tailEdges;
        }
      }
    }
    System.out.println(totalTails);
    System.out.println(totalHeads);
    tails = new int[totalTails];
    tailHeadOffsets = new int[totalTails];
    heads = new int[totalHeads];
    try (
            final DataInputStream tailInput = newFileDataInputStream(tailFile);
            final DataInputStream headInput = newFileDataInputStream(headFile)
    ) {
      int tailOffset = 0;
      int edgeOffset = 0;
      while (true) {
        final int tail;
        try {
          tail = tailInput.readInt();
        } catch (EOFException ignored) {
          break;
        }
        final int tailEdges = tailInput.readInt();
        if (tail % divisor == remainder) {
          tails[tailOffset] = tail;
          tailHeadOffsets[tailOffset] = edgeOffset;
          tailOffset++;
          edgeOffset += tailEdges;
        } else {
          headInput.skipBytes(tailEdges * Integer.BYTES);
        }
      }
    }
  }

  @Override
  public Stream<BreadthSearchGraph.VertexIdentifier> apply(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
    assert vertexIdentifier.id % divisor == remainder;
    final int tailOffset = Arrays.binarySearch(tails, vertexIdentifier.id);
    if (tailOffset < 0) {
      return Stream.empty();
    }
    return IntStream.range(
            tailHeadOffsets[tailOffset],
            tailOffset + 1 < tailHeadOffsets.length
                    ? tailHeadOffsets[tailOffset + 1]
                    : tailHeadOffsets[tailHeadOffsets.length - 1]
    ).mapToObj(index -> new BreadthSearchGraph.VertexIdentifier(heads[index]));
  }

  private static DataInputStream newFileDataInputStream(File file) throws IOException {
    final FileInputStream in = new FileInputStream(file);
    try {
      return new DataInputStream(new BufferedInputStream(in, 1 << 20));
    } catch (Throwable throwable) {
      in.close();
      throw throwable;
    }
  }
}
