package com.spbsu.flamestream.runtime.serialization;

import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import io.altoo.akka.serialization.kryo.DefaultKryoInitializer;
import io.altoo.akka.serialization.kryo.serializer.scala.ScalaKryo;

import java.lang.invoke.SerializedLambda;

public class AkkaKryoInitializer extends DefaultKryoInitializer {
  @Override
  public void preInit(ScalaKryo kryo) {
    kryo.register(Object[].class);
    kryo.register(Class.class);
    kryo.register(SerializedLambda.class);
    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
    kryo.setReferences(true);
  }
}
