package com.spbsu.flamestream.runtime.graph.api;

import com.spbsu.flamestream.core.DataItem;

public class AddressedItem {
  public enum Type {
    ANY, HASHED, BROADCAST
  }

  public final DataItem item;
  public final String vertexId;
  public final Type type;
  public final int hash;

  public AddressedItem(DataItem item, AddressedItem addressedItem) {
    this.item = item;
    vertexId = addressedItem.vertexId;
    type = addressedItem.type;
    hash = addressedItem.hash;
  }

  public AddressedItem(DataItem item, String vertexId, int hash) {
    this.item = item;
    this.vertexId = vertexId;
    type = Type.HASHED;
    this.hash = hash;
  }

  public AddressedItem(DataItem item, String vertexId, Type type, int hash) {
    this.item = item;
    this.vertexId = vertexId;
    this.type = type;
    this.hash = hash;
  }

  public DataItem item() {
    return item;
  }

  @Override
  public String toString() {
    return "AddressedItem(" + item + ", " + vertexId + ", " + type + ", " + hash  + ")";
  }
}
