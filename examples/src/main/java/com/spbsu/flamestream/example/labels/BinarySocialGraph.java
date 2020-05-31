package com.spbsu.flamestream.example.labels;

import com.spbsu.flamestream.core.graph.HashGroup;
import com.spbsu.flamestream.core.graph.HashUnit;
import org.jetbrains.annotations.Nullable;

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
import java.util.Random;
import java.util.RandomAccess;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BinarySocialGraph {
  private static final int TAIL_BYTES = Integer.BYTES + Long.BYTES;

  public final File tailFile, headFile;
  public final int minTail, maxTail;

  public BinarySocialGraph(File tailFile, File headFile) throws IOException {
    this.tailFile = tailFile;
    this.headFile = headFile;
    try (
            final FileInputStream tailInputFile = new FileInputStream(tailFile);
            final DataInputStream tailInput = new DataInputStream(tailInputFile)
    ) {
      minTail = tailInput.readInt();
      tailInputFile.getChannel().position(tailInputFile.getChannel().size() - TAIL_BYTES);
      maxTail = tailInput.readInt();
    }
  }

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

  public int size() {
    return (int) (tailFile.length() / TAIL_BYTES);
  }

  public final class CloseableTailsIterator implements Closeable {
    private final DataInputStream input;
    private final HashUnit hashUnit;
    private int vertex;
    private long headsOffset;

    public CloseableTailsIterator() throws IOException {
      this(null);
    }

    public CloseableTailsIterator(@Nullable HashUnit hashUnit) throws IOException {
      this.hashUnit = hashUnit;
      final int from = hashUnit == null ? 0 : Collections.binarySearch(new TailHashesList(tailFile), hashUnit.from());
      input = newBufferedFileDataInputStream(tailFile, (from < 0 ? -from + 1 : from) * TAIL_BYTES);
    }

    public boolean next() throws IOException {
      try {
        vertex = input.readInt();
      } catch (EOFException ignored) {
        headsOffset = headFile.length() / Integer.BYTES;
        return false;
      }
      headsOffset = input.readLong();
      return hashUnit == null || hashUnit.covers(BinarySocialGraph.this.hash(vertex));
    }

    @Override
    public void close() throws IOException {
      input.close();
    }

    public int vertex() {
      return vertex;
    }

    public long headsOffset() {
      return headsOffset;
    }
  }

  public final class BinaryOutboundEdges implements BreadthSearchGraph.HashedVertexEdges {
    final int[] tails, tailHeadOffsets, heads;
    private final HashGroup hashGroup;
    private final int limit;

    public BinaryOutboundEdges(HashGroup hashGroup, int limit) throws IOException {
      this.hashGroup = hashGroup;
      this.limit = limit;
      int totalTails = 0;
      int totalHeads = 0;
      for (final HashUnit unit : hashGroup.units()) {
        System.out.println(unit);
        try (final CloseableTailsIterator tailIterator = new CloseableTailsIterator(unit)) {
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
        try (final CloseableTailsIterator tailIterator = new CloseableTailsIterator(unit)) {
          if (tailIterator.next()) {
            try (final DataInputStream headsInput = newBufferedFileDataInputStream(
                    headFile,
                    tailIterator.headsOffset * Integer.BYTES
            )) {
              while (true) {
                final long headsOffset = tailIterator.headsOffset;
                tails[totalTails] = tailIterator.vertex;
                tailHeadOffsets[totalTails] = totalHeads;
                final boolean next = tailIterator.next();
                final int tailHeads = (int) (tailIterator.headsOffset - headsOffset);
                for (int i = 0; i < tailHeads; i++) {
                  heads[totalHeads++] = headsInput.readInt();
                }
                final Random random = new Random(tailIterator.vertex);
                for (int i = tailHeads - 1; i > 0; i--) {
                  final int index = random.nextInt(i + 1) + tailHeadOffsets[totalTails];
                  final int index2 = i + tailHeadOffsets[totalTails];
                  // Simple swap
                  final int tmp = heads[index];
                  heads[index] = heads[index2];
                  heads[index2] = tmp;
                }
                totalTails++;
                if (!next) {
                  break;
                }
              }
            }
          }
        }
      }
    }

    @Override
    public Stream<BreadthSearchGraph.VertexIdentifier> apply(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
      if (!hashGroup.covers(hash(vertexIdentifier))) {
        throw new IllegalArgumentException(hashGroup + " " + vertexIdentifier.id);
      }
      final int i = Arrays.binarySearch(tails, vertexIdentifier.id);
      if (i < 0) {
        return Stream.empty();
      }
      final int from = tailHeadOffsets[i], to = i + 1 < tailHeadOffsets.length ? tailHeadOffsets[i + 1] : heads.length;
      return IntStream.range(from, from + Integer.min(to - from, limit))
              .mapToObj(index -> new BreadthSearchGraph.VertexIdentifier(heads[index]));
    }

    @Override
    public int hash(BreadthSearchGraph.VertexIdentifier vertexIdentifier) {
      return BinarySocialGraph.this.hash(vertexIdentifier.id);
    }
  }

  private int hash(int tail) {
    return HashUnit.ALL.scale(tail, minTail, maxTail);
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
