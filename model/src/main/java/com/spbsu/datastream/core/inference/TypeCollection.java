package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;

import java.util.Collection;
import java.util.NoSuchElementException;

/**
 * Created by marnikitta on 28.11.16.
 */
public interface TypeCollection {
  DataType forName(String name) throws NoSuchElementException;

  void addMorphism(Morphism m);

  void addType(DataType type);

  Collection<Morphism> loadMorphisms();

  Collection<DataType> loadTypes();
}