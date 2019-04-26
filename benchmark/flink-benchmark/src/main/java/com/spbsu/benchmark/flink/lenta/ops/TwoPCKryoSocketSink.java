package com.spbsu.benchmark.flink.lenta.ops;

import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TwoPCKryoSocketSink extends TwoPhaseCommitSinkFunction<Prediction, String, Object> {
  private static final long serialVersionUID = 1L;
  private final KryoSocketSink socketSink;

  private final Map<String, Collection<Prediction>> openedTransactions = new HashMap<>();

  public TwoPCKryoSocketSink(String hostName, int port, ExecutionConfig config) {
    super(
            new StringSerializer(),
            new GenericTypeInfo<>(Object.class).createSerializer(config)
    );
    socketSink = new KryoSocketSink(hostName, port);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    socketSink.open(parameters);
  }

  @Override
  protected void invoke(String transaction, Prediction value, Context context) {
    openedTransactions.get(transaction).add(value);
  }

  @Override
  protected String beginTransaction() {
    final String s = UUID.randomUUID().toString();
    openedTransactions.put(s, new ArrayList<>());
    return s;
  }

  @Override
  protected void preCommit(String transaction) {
  }

  @Override
  protected void commit(String transaction) {
    openedTransactions.get(transaction).forEach(result -> socketSink.invoke(result, null));
    openedTransactions.remove(transaction);
  }

  @Override
  protected void abort(String transaction) {
    openedTransactions.remove(transaction);
  }

  @Override
  public void close() {
    socketSink.close();
  }
}

