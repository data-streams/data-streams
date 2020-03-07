package com.spbsu.flamestream.example.bl.text_classifier;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordEntry;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierOutput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
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
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TextClassifierGraph implements Supplier<Graph> {
  private final Vectorizer vectorizer;
  private final OnlineModel onlineModel;

  TextClassifierGraph(Vectorizer vectorizer, OnlineModel onlineModel) {
    this.vectorizer = vectorizer;
    this.onlineModel = onlineModel;
  }

  private final HashFunction wordHash = HashFunction.uniformHash(
          dataItem -> dataItem.payload(WordContainer.class).word().hashCode()
  );

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

  private final Equalz equalzWord = (o1, o2) ->
          o1.payload(WordContainer.class).word().equals(o2.payload(WordContainer.class).word());

  private final Equalz equalzDoc = (o1, o2) -> o1.payload(DocContainer.class)
          .partitioning()
          .equals(o2.payload(DocContainer.class).partitioning());

  @Override
  public Graph get() {
    final Source source = new Source();
    final Grouping<WordContainer> groupingWord =
            new Grouping<>(wordHash, equalzWord, 2, WordContainer.class);
    final Grouping<DocContainer> groupingTfIdf =
            new Grouping<>(docHash, equalzDoc, 2, DocContainer.class);


    final FlameMap<TfObject, WordEntry> splitterWord = new FlameMap.Builder<>(
            (TfObject tfObject) ->
                    tfObject.counts().entrySet().stream()
                            .map(word -> new WordEntry(
                                    word.getKey(),
                                    tfObject.document(),
                                    tfObject.counts().size(),
                                    tfObject.partitioning()
                            )),
            TfObject.class
    ).build();


    final FlameMap<TextDocument, TfObject> splitterTf =
            new FlameMap.Builder<TextDocument, TfObject>(text -> Stream.of(TfObject.ofText(text)), TextDocument.class)
                    .hashFunction(docHash)
                    .build();

    final FlameMap<List<WordContainer>, List<WordContainer>> filterWord =
            new FlameMap.Builder<>(new WordContainerOrderingFilter(), List.class).build();

    final FlameMap<List<WordContainer>, WordCounter> counterWord =
            new FlameMap.Builder<>(new CountWordEntries(), List.class).build();

    final FlameMap<List<DocContainer>, DocContainer> filterTfIdf =
            new FlameMap.Builder<>(new TfIdfFilter(), List.class).build();


    final ClassifierFilter classifier = new ClassifierFilter(vectorizer, onlineModel);
    final FlameMap<List<ClassifierInput>, ClassifierOutput> filterClassifier =
            new FlameMap.Builder<>(classifier, List.class).init(__1 -> classifier.init()).build();

    final IDFObjectCompleteFilter completeFilter = new IDFObjectCompleteFilter();
    final FlameMap<WordCounter, IdfObject> idfObjectCompleteFilter =
            new FlameMap.Builder<>(completeFilter, WordCounter.class)
                    .hashFunction(docHash).init(__ -> completeFilter.init())
                    .build();


    final FlameMap<TfIdfObject, ClassifierInput> labeledFilter =
            new FlameMap.Builder<>(new LabeledFilter(), TfIdfObject.class).build();

    final FlameMap<TfIdfObject, ClassifierInput> nonLabeledFilter =
            new FlameMap.Builder<>(new NonLabeledFilter(), TfIdfObject.class).build();

    final FlameMap<ClassifierOutput, ClassifierInput> weightsFilter =
            new FlameMap.Builder<>(new WeightsFilter(), ClassifierOutput.class).build();

    final FlameMap<ClassifierOutput, Prediction> predictionFilter =
            new FlameMap.Builder<>(new PredictionFilter(), ClassifierOutput.class).build();

    final Grouping<ClassifierInput> groupingWeights =
            new Grouping<>(HashFunction.PostBroadcast.INSTANCE, Equalz.allEqualz(), 2, ClassifierInput.class);

    final FlameMap<ClassifierInput, ClassifierInput> broadcastTfidfObject =
            new FlameMap.Builder<ClassifierInput, ClassifierInput>(Stream::of, ClassifierInput.class)
                    .hashFunction(HashFunction.Broadcast.INSTANCE)
                    .build();

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
