package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.graph.HashGroup;
import com.spbsu.flamestream.core.graph.HashUnit;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.RandomAccess;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class BinaryOutboundEdges implements BreadthSearchGraph.HashedVertexEdges {
  private static final int TAIL_BYTES = Integer.BYTES + Long.BYTES;

  final int[] tails, tailHeadOffsets, heads;
  final int minTail, maxTail;

  private class TailHashesList extends AbstractList<Integer> implements RandomAccess {
    final RandomAccessFile file;
    final int size;

    private TailHashesList(File file) throws FileNotFoundException {
      this.file = new RandomAccessFile(file, "r");
      size = (int) (file.length() / TAIL_BYTES);
    }

    @Override
    public Integer get(int i) {
      try {
        file.seek(i * TAIL_BYTES);
        return hash(file.readInt());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int size() {
      return size;
    }
  }

  private final class CloseableTailsIterator implements Closeable {
    final DataInputStream input;
    File headFile;
    HashUnit hashUnit;

    CloseableTailsIterator(File tailFile, File headFile, HashUnit hashUnit) throws IOException {
      this.headFile = headFile;
      this.hashUnit = hashUnit;
      final int from = Collections.binarySearch(new TailHashesList(tailFile), hashUnit.from());
      input = newBufferedFileDataInputStream(tailFile, (from < 0 ? -from + 1 : from) * TAIL_BYTES);
    }

    boolean next() throws IOException {
      try {
        tail = input.readInt();
      } catch (EOFException ignored) {
        headsOffset = headFile.length() / Integer.BYTES;
        return false;
      }
      headsOffset = input.readLong();
      return hashUnit.covers(hash(tail));
    }

    int tail;
    long headsOffset;

    @Override
    public void close() throws IOException {
      input.close();
    }
  }

  public BinaryOutboundEdges(File tailFile, File headFile, HashGroup hashGroup) throws IOException {
    try (
            final FileInputStream tailInputFile = new FileInputStream(tailFile);
            final DataInputStream tailInput = new DataInputStream(tailInputFile)
    ) {
      minTail = tailInput.readInt();
      tailInputFile.getChannel().position(tailInputFile.getChannel().size() - TAIL_BYTES);
      maxTail = tailInput.readInt();
    }
    int totalTails = 0;
    int totalHeads = 0;
    for (final HashUnit unit : hashGroup.units()) {
      System.out.println(unit);
      try (final CloseableTailsIterator tailIterator = new CloseableTailsIterator(tailFile, headFile, unit)) {
        if (tailIterator.next()) {
          totalTails++;
          final long fromHeadsOffset = tailIterator.headsOffset;
          while (tailIterator.next()) {
            totalTails++;
          }
          totalHeads += tailIterator.headsOffset - fromHeadsOffset;
        }
      }
    }
    System.out.println("totalTails " + totalTails + ", totalHeads " + totalHeads);
    tails = new int[totalTails];
    tailHeadOffsets = new int[totalTails];
    heads = new int[totalHeads];
    totalTails = totalHeads = 0;
    for (final HashUnit unit : hashGroup.units()) {
      try (final CloseableTailsIterator tailIterator = new CloseableTailsIterator(tailFile, headFile, unit)) {
        if (tailIterator.next()) {
          try (final DataInputStream headsInput = newBufferedFileDataInputStream(
                  headFile,
                  (long) tailIterator.headsOffset * Integer.BYTES
          )) {
            while (true) {
              final long headsOffset = tailIterator.headsOffset;
              tails[totalTails] = tailIterator.tail;
              tailHeadOffsets[totalTails] = totalHeads;
              totalTails++;
              final boolean next = tailIterator.next();
              final int tailHeads = (int) (tailIterator.headsOffset - headsOffset);
              for (int i = 0; i < tailHeads; i++) {
                heads[totalHeads++] = headsInput.readInt();
              }
              if (!next) {
                break;
              }
            }
            ;
          }
        }
      }
    }
  }

  @Override
  public Stream<BreadthSearchGraph.VertexIdentifier> apply(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
    final int i = Arrays.binarySearch(tails, vertexIdentifier.id);
    if (i < 0) {
      return Stream.empty();
    }
    final int from = tailHeadOffsets[i], to = i + 1 < tailHeadOffsets.length ? tailHeadOffsets[i + 1] : heads.length;
    return IntStream.range(from, to).mapToObj(index -> new BreadthSearchGraph.VertexIdentifier(heads[index]));
  }

  @Override
  public int hash(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
    return hash(vertexIdentifier.id);
  }

  private int hash(int tail) {
    return HashUnit.scale(tail, minTail, maxTail);
  }

  private static DataInputStream newBufferedFileDataInputStream(File file, long from) throws IOException {
    final FileInputStream in = new FileInputStream(file);
    in.getChannel().position(from);
    try {
      return new DataInputStream(new BufferedInputStream(in, 1 << 20));
    } catch (Throwable throwable) {
      in.close();
      throw throwable;
    }
  }
}
