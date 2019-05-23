package com.spbsu.flamestream.runtime.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class KryoSerializer implements FlameSerializer {
  private static final int MAX_BUFFER_SIZE = 20000;
  private static final int BUFFER_SIZE = 1000;

  private final static ThreadLocal<Kryo> KRYO_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
    final Kryo kryo = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
    kryo.getFieldSerializerConfig().setIgnoreSyntheticFields(false);
    return kryo;
  });

  @Override
  public byte[] serialize(Object o) {
    final ByteBufferOutput bufferOutput = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
    KRYO_THREAD_LOCAL.get().writeClassAndObject(bufferOutput, o);
    return bufferOutput.toBytes();
  }

  @Override
  public <T> T deserialize(byte[] data, Class<T> clazz) {
    final ByteBufferInput input = new ByteBufferInput(data);
    //noinspection unchecked
    return (T) KRYO_THREAD_LOCAL.get().readClassAndObject(input);
  }
}
