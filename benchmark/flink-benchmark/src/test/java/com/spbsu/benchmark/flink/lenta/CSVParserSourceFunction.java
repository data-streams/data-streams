package com.spbsu.benchmark.flink.lenta;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Preconditions;

import java.io.Reader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class CSVParserSourceFunction implements SourceFunction<CSVRecord>, CheckpointedFunction, Serializable {
  /**
   * The number of serialized elements.
   */
  private final Supplier<Reader> readerSupplier;
  private final CSVFormat csvFormat;

  /**
   * Flag to make the source cancelable.
   */
  private volatile boolean isRunning = true;

  private transient ListState<Tuple2<Long, Long>> checkpointedState;

  private long characterOffset = 0, recordNumber = 0;

  public CSVParserSourceFunction(Supplier<Reader> readerSupplier, CSVFormat csvFormat) {
    this.readerSupplier = readerSupplier;
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
                    new TupleSerializer(
                            Tuple2.class,
                            new TypeSerializer[]{LongSerializer.INSTANCE, LongSerializer.INSTANCE}
                    )
            )
    );

    if (context.isRestored()) {
      List<Tuple2<Long, Long>> retrievedStates = new ArrayList<>();
      for (Tuple2<Long, Long> entry : this.checkpointedState.get()) {
        retrievedStates.add(entry);
      }

      // given that the parallelism of the function is 1, we can only have 1 state
      Preconditions.checkArgument(
              retrievedStates.size() == 1,
              getClass().getSimpleName() + " retrieved invalid state."
      );

      characterOffset = retrievedStates.get(0).f0;
      recordNumber = retrievedStates.get(0).f1;
    }
  }

  @Override
  public void run(SourceContext<CSVRecord> ctx) throws Exception {
    try (CSVParser csvParser = new CSVParser(
            readerSupplier.get(),
            characterOffset == 0 ? csvFormat.withFirstRecordAsHeader() : csvFormat,
            characterOffset,
            recordNumber
    )) {
      Iterator<CSVRecord> iterator = csvParser.iterator();
      final Object lock = ctx.getCheckpointLock();

      while (isRunning && iterator.hasNext()) {
        CSVRecord next = iterator.next();
        synchronized (lock) {
          ctx.collect(next);
          characterOffset = next.getCharacterPosition();
          recordNumber = next.getRecordNumber();
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
    this.checkpointedState.add(new Tuple2<>(characterOffset, recordNumber));
  }
}