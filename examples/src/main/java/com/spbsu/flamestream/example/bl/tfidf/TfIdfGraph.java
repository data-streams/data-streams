package com.spbsu.flamestream.example.bl.tfidf;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.tfidf.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidf.model.Prediction;
import com.spbsu.flamestream.example.bl.tfidf.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.tfidf.model.TextDocument;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.entries.WordEntry;
import com.spbsu.flamestream.example.bl.tfidf.ops.entries.CountWordEntries;
import com.spbsu.flamestream.example.bl.tfidf.ops.entries.IDFAggregator;
import com.spbsu.flamestream.example.bl.tfidf.ops.entries.TrainAggregator;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.Classifier;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.DocContainerOrderingFilter;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.IDFObjectCompleteFilter;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.TfIdfFilter;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.TrainOrderingFilter;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.WordContainerOrderingFilter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class TfIdfGraph implements Supplier<Graph> {

  private final HashFunction wordHash = HashFunction.uniformHash(new HashFunction() {
    @Override
    public int hash(DataItem dataItem) {
      return HashFunction.objectHash(WordContainer.class).applyAsInt(dataItem);
    }
  });

  private final HashFunction docHash = HashFunction.uniformHash(new HashFunction() {
    @Override
    public int hash(DataItem dataItem) {
      return dataItem.payload(DocContainer.class).partitioning().hashCode();
    }

    @Override
    public String toString() {
      return "PHash";
    }
  });

  private final HashFunction constHash = HashFunction.uniformHash(new HashFunction() {
    @Override
    public int hash(DataItem dataItem) {
      return 345;
    }

    @Override
    public String toString() {
      return "ConstHash";
    }
  });

  @SuppressWarnings("Convert2Lambda")
  private final Equalz equalzWord = new Equalz() {
    @Override
    public boolean test(DataItem o1, DataItem o2) {
      return o1.payload(WordContainer.class).word().equals(o2.payload(WordContainer.class).word());
    }
  };

  @SuppressWarnings("Convert2Lambda")
  private final Equalz equalzDoc = new Equalz() {
    @Override
    public boolean test(DataItem o1, DataItem o2) {
      return o1.payload(DocContainer.class).partitioning().equals(o2.payload(DocContainer.class).partitioning());
    }
  };

  @SuppressWarnings("Convert2Lambda")
  private final Equalz equalzAll = new Equalz() {
    @Override
    public boolean test(DataItem o1, DataItem o2) {
      return true;
    }
  };


  @Override
  public Graph get() {
    final Source source = new Source();
    final Grouping<WordContainer> groupingWord =
            new Grouping<>(wordHash, equalzWord, 2, WordContainer.class);

    final Grouping<DocContainer> groupingDoc =
            new Grouping<>(docHash, equalzDoc, 2, DocContainer.class);

    final Grouping<DocContainer> gropingTfIdf =
            new Grouping<>(docHash, equalzDoc, 2, DocContainer.class);

    final Grouping<Object> groupingPredictionBatch =
            new Grouping<>(constHash, equalzAll, 2, Object.class);



    final FlameMap<TextDocument, WordEntry> splitterWord = new FlameMap<>(new Function<TextDocument, Stream<WordEntry>>() {
      @Override
      public Stream<WordEntry> apply(TextDocument s) {
        final Map<String, Integer> counter = new HashMap<>();
        for (String w : TextUtils.words(s.content())) {
          counter.put(w, counter.getOrDefault(w, 0) + 1);
        }
        return counter.entrySet().stream()
                .map(word -> new WordEntry(word.getKey(), s.name(), counter.size(), s.partitioning()));
      }
    }, TextDocument.class);


    final FlameMap<TextDocument, TfIdfObject> splitterTF = new FlameMap<>(new Function<TextDocument, Stream<TfIdfObject>>() {
      @Override
      public Stream<TfIdfObject> apply(TextDocument text) {
        TfIdfObject tfIdfObject = TfIdfObject.ofText(text);
        return Stream.of(tfIdfObject);
      }
    }, TextDocument.class);

    final FlameMap<List<WordContainer>, List<WordContainer>> filterWord = new FlameMap<>(
            new WordContainerOrderingFilter(),
            List.class
    );

    final FlameMap<List<WordContainer>, WordCounter> counterWord = new FlameMap<>(
            new CountWordEntries(),
            List.class
    );

    final FlameMap<List<DocContainer>, DocContainer> idfAggregator = new FlameMap<>(
            new IDFAggregator(),
            List.class
    );

    final FlameMap<List<Object>, List<TfIdfObject>> trainAggregator = new FlameMap<>(
            new TrainAggregator(),
            List.class
    );

    final FlameMap<List<DocContainer>, List<DocContainer>> filterDoc = new FlameMap<>(
            new DocContainerOrderingFilter(),
            List.class
    );

    final FlameMap<List<DocContainer>, DocContainer> filterTfIdf = new FlameMap<>(
            new TfIdfFilter(),
            List.class
    );

    final FlameMap<List<Object>, List<Object>> filterTrainer = new FlameMap<List<Object>, List<Object>>(
            new TrainOrderingFilter(),
            List.class
    );


    final FlameMap<TfIdfObject, TfIdfObject> filterToClassify = new FlameMap<>(new Function<TfIdfObject, Stream<TfIdfObject>>() {
      @Override
      public Stream<TfIdfObject> apply(TfIdfObject tfIdfObject) {
        return tfIdfObject.topics() == null ? Stream.of(tfIdfObject) : Stream.of();
      }
    }, TfIdfObject.class);

    final FlameMap<TfIdfObject, TfIdfObject> filterToPredict = new FlameMap<>(new Function<TfIdfObject, Stream<TfIdfObject>>() {
      @Override
      public Stream<TfIdfObject> apply(TfIdfObject tfIdfObject) {
        System.out.println("??? toPredict: " + tfIdfObject.document());
        return tfIdfObject.topics() != null ? Stream.of(tfIdfObject) : Stream.of();
      }
    }, TfIdfObject.class);


    final FlameMap<List<Object>, List<Object>> filterPredictBatch = new FlameMap<>(new Function<List<Object>, Stream<List<Object>>>() {
      @Override
      public Stream<List<Object>> apply(List<Object> tfIdfObjects) {
        System.out.println("??? toPredictBatch: " + tfIdfObjects.size());
        System.out.println("?????? toPredictBatch: " + tfIdfObjects);

        return tfIdfObjects.size() >= 5 ? Stream.of(tfIdfObjects) : Stream.of();
      }
    }, List.class);


    final Classifier classifier = new Classifier();
    final FlameMap<TfIdfObject, Prediction> filterClassifier = new FlameMap<>(
            classifier,
            TfIdfObject.class,
            new Runnable() {
              @Override
              public void run() {
                classifier.init();
              }
            }
    );

    final FlameMap<IDFObject, IDFObject> idfObjectCompleteFilter = new FlameMap<>(
            new IDFObjectCompleteFilter(),
            IDFObject.class
    );

    final Sink sink = new Sink();
    return new Graph.Builder()
            .link(source, splitterTF)
            .link(source, splitterWord)

            .link(splitterWord, groupingWord)
            .link(groupingWord, filterWord)
            .link(filterWord, counterWord)
            .link(counterWord, groupingWord)

            .link(counterWord, groupingDoc)
            .link(groupingDoc, filterDoc)
            .link(filterDoc, idfAggregator)
            .link(idfAggregator, groupingDoc)

            .link(idfAggregator, idfObjectCompleteFilter)

            .link(idfObjectCompleteFilter, gropingTfIdf)
            .link(splitterTF, gropingTfIdf)
            .link(gropingTfIdf, filterTfIdf)

            .colocate(groupingDoc, filterDoc, idfAggregator, idfObjectCompleteFilter, gropingTfIdf,
                    filterTfIdf)
            .colocate(groupingWord, filterWord, counterWord)

            .link(filterTfIdf, filterToClassify)
            .link(filterTfIdf, filterToPredict)
            .link(filterToClassify, filterClassifier)
            .link(filterClassifier, sink)

            .link(filterToPredict, groupingPredictionBatch)
            .link(groupingPredictionBatch, filterTrainer)
            .link(filterTrainer, trainAggregator)
            .link(trainAggregator, groupingPredictionBatch)

            .colocate(groupingPredictionBatch, filterTrainer, trainAggregator)

            .link(trainAggregator, filterPredictBatch)

            .build(source, sink);
  }
}
