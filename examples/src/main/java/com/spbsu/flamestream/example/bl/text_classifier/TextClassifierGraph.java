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
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierOutput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.ClassifierFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.CountWordEntries;
import com.spbsu.flamestream.example.bl.text_classifier.ops.IDFObjectCompleteFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.LabeledFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.NonLabeledFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.PredictionFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.TfIdfFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.WeightsFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.WordContainerOrderingFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.OnlineModel;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TextClassifierGraph implements Supplier<Graph> {
  private final Vectorizer vectorizer;
  private final OnlineModel onlineModel;

  public TextClassifierGraph(Vectorizer vectorizer, OnlineModel onlineModel) {
    this.vectorizer = vectorizer;
    this.onlineModel = onlineModel;
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
    final Grouping<DocContainer> groupingTfIdf =
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

    final ClassifierFilter classifier = new ClassifierFilter(vectorizer, onlineModel);
    //noinspection Convert2Lambda,Anonymous2MethodRef
    final FlameMap<List<ClassifierInput>, ClassifierOutput> filterClassifier = new FlameMap<>(
            classifier,
            List.class,
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


    final FlameMap<TfIdfObject, ClassifierInput> labeledFilter = new FlameMap<>(
            new LabeledFilter(),
            TfIdfObject.class
    );

    final FlameMap<TfIdfObject, ClassifierInput> nonLabeledFilter = new FlameMap<>(
            new NonLabeledFilter(),
            TfIdfObject.class
    );

    final FlameMap<ClassifierOutput, ClassifierInput> weightsFilter = new FlameMap<>(
            new WeightsFilter(),
            ClassifierOutput.class
    );

    final FlameMap<ClassifierOutput, Prediction> predictionFilter = new FlameMap<>(
            new PredictionFilter(),
            ClassifierOutput.class
    );

    final Grouping<ClassifierInput> groupingWeights =
            new Grouping<>(HashFunction.broadcastBeforeGroupingHash(), Equalz.allEqualz(), 2, ClassifierInput.class);

    //noinspection Convert2Lambda,Anonymous2MethodRef
    final FlameMap<ClassifierInput, ClassifierInput> broadcastTfidfObject = new FlameMap<>(
            new Function<ClassifierInput, Stream<ClassifierInput>>() {
              @Override
              public Stream<ClassifierInput> apply(ClassifierInput t) {
                return Stream.of(t);
              }
            },
            ClassifierInput.class,
            HashFunction.broadcastHash()
    );

    final Sink sink = new Sink();
    return new Graph.Builder()
            .link(source, splitterTf)
            .link(splitterTf, groupingTfIdf)
            .link(splitterTf, splitterWord)

            .link(splitterWord, groupingWord)
            .link(groupingWord, filterWord)
            .link(filterWord, counterWord)
            .link(counterWord, groupingWord)

            .link(counterWord, idfObjectCompleteFilter)
            .link(idfObjectCompleteFilter, groupingTfIdf)
            .link(groupingTfIdf, filterTfIdf)

            .link(filterTfIdf, nonLabeledFilter)
            .link(nonLabeledFilter, groupingWeights)

            .link(groupingWeights, filterClassifier)
            .link(filterClassifier, weightsFilter)
            .link(weightsFilter, groupingWeights)
            .link(filterClassifier, predictionFilter)
            .link(predictionFilter, sink)

            .link(filterTfIdf, labeledFilter)
            .link(labeledFilter, broadcastTfidfObject)
            .link(broadcastTfidfObject, groupingWeights)

            .colocate(splitterTf, splitterWord)
            .colocate(groupingWord, filterWord, counterWord)
            .colocate(
                    idfObjectCompleteFilter,
                    groupingTfIdf,
                    filterTfIdf,
                    labeledFilter
            )
            .colocate(
                    nonLabeledFilter,
                    broadcastTfidfObject,
                    groupingWeights,
                    filterClassifier,
                    predictionFilter,
                    weightsFilter,
                    sink
            )

            .build(source, sink);
  }
}
