package com.spbsu.benchmark.flink.lenta;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.BoundedInputStream;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;
import uk.ac.ebi.pride.tools.braf.BufferedRandomAccessFile;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class CSVParserSourceFunction implements SourceFunction<CSVRecord>, CheckpointedFunction, Serializable {
  private final File file;
  /**
   * The number of serialized elements.
   */
  private final CSVFormat csvFormat;

  /**
   * Flag to make the source cancelable.
   */
  private volatile boolean isRunning = true;

  private transient ListState<Tuple3<Long, Long, Long>> checkpointedState;

  private long filePos = 0, characterOffset = 0, recordNumber = 0;

  public CSVParserSourceFunction(File file, CSVFormat csvFormat) {
    this.file = file;
    this.csvFormat = csvFormat;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    Preconditions.checkState(
            this.checkpointedState == null,
            "The " + getClass().getSimpleName() + " has already been initialized."
    );
    this.checkpointedState = context.getOperatorStateStore().getListState(
            new ListStateDescriptor<>(
                    "from-elements-state",
                    new TupleSerializer(Tuple3.class, new TypeSerializer[]{
                            LongSerializer.INSTANCE,
                            LongSerializer.INSTANCE,
                            LongSerializer.INSTANCE
                    })
            )
    );

    if (context.isRestored()) {
      List<Tuple3<Long, Long, Long>> retrievedStates = new ArrayList<>();
      for (Tuple3<Long, Long, Long> entry : this.checkpointedState.get()) {
        retrievedStates.add(entry);
      }

      // given that the parallelism of the function is 1, we can only have 1 state
      Preconditions.checkArgument(
              retrievedStates.size() == 1,
              getClass().getSimpleName() + " retrieved invalid state."
      );

      filePos = retrievedStates.get(0).f0;
      characterOffset = retrievedStates.get(0).f1;
      recordNumber = retrievedStates.get(0).f2;
    }
  }

  @Override
  public void run(SourceContext<CSVRecord> ctx) throws Exception {
    System.out.println(filePos);
    System.out.println(characterOffset);
    System.out.println(recordNumber);
    try (final BufferedRandomAccessFile file = new BufferedRandomAccessFile(this.file, "r", 8192)) {
      file.seek(filePos);
      try (CSVParser csvParser = new CSVParser(
              new InputStreamReader(
                      new InputStream() {
                        @Override
                        public int read() throws IOException {
                          return file.read();
                        }
                      },
                      StandardCharsets.UTF_8
              ),
              filePos == 0 ? csvFormat.withFirstRecordAsHeader() : csvFormat,
              characterOffset,
              recordNumber
      )) {
        Iterator<CSVRecord> iterator = csvParser.iterator();
        final Object lock = ctx.getCheckpointLock();

        while (isRunning && iterator.hasNext()) {
          CSVRecord next = iterator.next();
          synchronized (lock) {
            ctx.collect(next);
            filePos = file.getFilePointer();
            characterOffset = next.getCharacterPosition();
            recordNumber = next.getRecordNumber();
          }
        }
      }
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  // ------------------------------------------------------------------------
  //  Checkpointing
  // ------------------------------------------------------------------------

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    Preconditions.checkState(
            this.checkpointedState != null,
            "The " + getClass().getSimpleName() + " has not been properly initialized."
    );

    this.checkpointedState.clear();
    this.checkpointedState.add(new Tuple3<>(filePos, characterOffset, recordNumber));
  }
}
