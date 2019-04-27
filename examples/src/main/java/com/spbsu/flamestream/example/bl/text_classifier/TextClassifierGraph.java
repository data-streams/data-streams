package com.spbsu.flamestream.example.bl.text_classifier;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordEntry;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.entries.CountWordEntries;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.Classifier;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.IDFObjectCompleteFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.TfIdfFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.WordContainerOrderingFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TextClassifierGraph implements Supplier<Graph> {
  private final TopicsPredictor predictor;

  public TextClassifierGraph(TopicsPredictor predictor) {
    this.predictor = predictor;
  }

  @SuppressWarnings("Convert2Lambda")
  private final HashFunction wordHash = HashFunction.uniformHash(new HashFunction() {
    @Override
    public int hash(DataItem dataItem) {
      return dataItem.payload(WordContainer.class).word().hashCode();
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

  @Override
  public Graph get() {
    final Source source = new Source();
    final Grouping<WordContainer> groupingWord =
            new Grouping<>(wordHash, equalzWord, 2, WordContainer.class);
    final Grouping<DocContainer> gropingTfIdf =
            new Grouping<>(docHash, equalzDoc, 2, DocContainer.class);


    //noinspection Convert2Lambda
    final FlameMap<TfObject, WordEntry> splitterWord = new FlameMap<>(new Function<TfObject, Stream<WordEntry>>() {
      @Override
      public Stream<WordEntry> apply(TfObject tfObject) {
        return tfObject.counts().entrySet().stream()
                .map(word -> new WordEntry(
                        word.getKey(),
                        tfObject.document(),
                        tfObject.counts().size(),
                        tfObject.partitioning()
                ));
      }
    }, TfObject.class);


    //noinspection Convert2Lambda
    final FlameMap<TextDocument, TfObject> splitterTf = new FlameMap<>(new Function<TextDocument, Stream<TfObject>>() {
      @Override
      public Stream<TfObject> apply(TextDocument text) {
        TfObject tfObject = TfObject.ofText(text);
        return Stream.of(tfObject);
      }
    }, TextDocument.class, docHash);

    final FlameMap<List<WordContainer>, List<WordContainer>> filterWord = new FlameMap<>(
            new WordContainerOrderingFilter(),
            List.class
    );

    final FlameMap<List<WordContainer>, WordCounter> counterWord = new FlameMap<>(
            new CountWordEntries(),
            List.class
    );

    final FlameMap<List<DocContainer>, DocContainer> filterTfIdf = new FlameMap<>(
            new TfIdfFilter(),
            List.class
    );

    final ClassifierFunction classifier = new ClassifierFunction(new Classifier(predictor));
    //noinspection Convert2Lambda,Anonymous2MethodRef
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

    final IDFObjectCompleteFilter completeFilter = new IDFObjectCompleteFilter();
    //noinspection Convert2Lambda,Anonymous2MethodRef
    final FlameMap<WordCounter, IdfObject> idfObjectCompleteFilter = new FlameMap<>(
            completeFilter,
            WordCounter.class,
            docHash,
            new Runnable() {
              @Override
              public void run() {
                completeFilter.init();
              }
            }
    );

    final Sink sink = new Sink();
    return new Graph.Builder()
            .link(source, splitterTf)
            .link(splitterTf, gropingTfIdf)
            .link(splitterTf, splitterWord)

            .link(splitterWord, groupingWord)
            .link(groupingWord, filterWord)
            .link(filterWord, counterWord)
            .link(counterWord, groupingWord)

            .link(counterWord, idfObjectCompleteFilter)
            .link(idfObjectCompleteFilter, gropingTfIdf)
            .link(gropingTfIdf, filterTfIdf)
            .link(filterTfIdf, filterClassifier)
            .link(filterClassifier, sink)

            .colocate(splitterTf, splitterWord)
            .colocate(groupingWord, filterWord, counterWord)
            .colocate(
                    idfObjectCompleteFilter,
                    gropingTfIdf,
                    filterTfIdf,
                    filterClassifier,
                    sink
            )

            .build(source, sink);
  }

  private static class ClassifierFunction implements Function<TfIdfObject, Stream<Prediction>> {
    private final Classifier classifier;

    public ClassifierFunction(Classifier classifier) {
      this.classifier = classifier;
    }

    public void init() {
      classifier.init();
    }

    @Override
    public Stream<Prediction> apply(TfIdfObject tfIdfObject) {
      return Stream.of(classifier.apply(tfIdfObject));
    }
  }
}
