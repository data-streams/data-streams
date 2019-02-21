package com.spbsu.flamestream.example.bl.tfidf.ops.entries;

import com.spbsu.flamestream.example.bl.tfidf.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidf.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordCounter;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Stream;

public class TrainAggregator implements Function<List<Object>, Stream<List<TfIdfObject>>> {
  @Override
  public Stream<List<TfIdfObject>> apply(List<Object> elems) {
    System.out.println("TAAA:" + elems);

    if (elems.size() == 1) {
      System.out.println("111111111");
      //final WordCounter wordCounter = (WordCounter) docContainers.get(0);
      List<TfIdfObject> l = new CopyOnWriteArrayList<>();
      l.add((TfIdfObject) elems.get(0));
      return Stream.of(l);
    } else {
      System.out.println("22222");
      final List lst = (List) elems.get(0);
      final TfIdfObject tfIdfObject = (TfIdfObject) elems.get(1);
      lst.add(tfIdfObject);
      System.out.println("22222: " + lst.size());

      //IDFObject result = new IDFObject(counter, new IDFObject(wordCounter));
      return Stream.of(lst);
    }
  }
}
