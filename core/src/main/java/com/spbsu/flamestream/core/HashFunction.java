package com.spbsu.flamestream.core;

import com.google.common.hash.Hashing;

import java.util.function.ToIntFunction;

@FunctionalInterface
@SuppressWarnings("Convert2Lambda")
public interface HashFunction extends ToIntFunction<DataItem> {
  Broadcast BROADCAST = new Broadcast();
  int BROADCAST_GROUPING_HASH = Integer.MAX_VALUE;

  static <T> HashFunction objectHash(Class<T> clazz) {
    return new HashFunction() {
      private final Class<T> c = clazz;

      @Override
      public int hash(DataItem item) {
        return item.payload(c).hashCode();
      }
    };
  }

  static HashFunction uniformHash(HashFunction hashFunction) {
    return new HashFunction() {
      private final HashFunction h = hashFunction;

      @Override
      public int hash(DataItem item) {
        return Hashing.murmur3_32().hashInt(h.applyAsInt(item)).asInt();
      }
    };
  }

  static HashFunction constantHash(int hash) {
    return new HashFunction() {
      private final int h = hash;

      @Override
      public int hash(DataItem item) {
        return h;
      }
    };
  }

  static HashFunction broadcastBeforeGroupingHash() {
    return new HashFunction() {
      @Override
      public int hash(DataItem item) {
        return BROADCAST_GROUPING_HASH;
      }
    };
  }

  static HashFunction bucketedHash(HashFunction hash, int buckets) {
    return new HashFunction() {
      private final HashFunction h = hash;
      private final int b = buckets;

      @Override
      public int hash(DataItem item) {
        return h.applyAsInt(item) % b;
      }
    };
  }

  static HashFunction broadcastHash() {
    return BROADCAST;
  }

  int hash(DataItem dataItem);

  @Override
  default int applyAsInt(DataItem dataItem) {
    return hash(dataItem);
  }

  class Broadcast implements HashFunction {

    private Broadcast() {

    }

    @Override
    public int hash(DataItem dataItem) {
      return 0; //this value is not used
    }
  }
}
