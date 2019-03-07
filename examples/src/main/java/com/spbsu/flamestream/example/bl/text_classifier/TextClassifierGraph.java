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
import com.spbsu.flamestream.example.bl.text_classifier.model.ModelParameters;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TrainInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordEntry;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.WordContainer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.entries.CountWordEntries;
import com.spbsu.flamestream.example.bl.text_classifier.ops.entries.IDFAggregator;
import com.spbsu.flamestream.example.bl.text_classifier.ops.entries.ModelAggregator;
import com.spbsu.flamestream.example.bl.text_classifier.ops.entries.TrainAggregator;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.Classifier;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.DocContainerOrderingFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.IDFObjectCompleteFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.ModelParametersFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.TfIdfFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.TrainOrderingFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.TrainFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.WordContainerOrderingFilter;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TextClassifierGraph implements Supplier<Graph> {
  private final TopicsPredictor predictor;

  TextClassifierGraph(TopicsPredictor predictor) {
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

  private final HashFunction constHash2 = HashFunction.uniformHash(new HashFunction() {
    @Override
    public int hash(DataItem dataItem) {
      return 123;
    }

    @Override
    public String toString() {
      return "ConstHash-2";
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

    final Grouping<Object> groupingModelParameters =
            new Grouping<>(constHash2, equalzAll, 2, Object.class);

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

    final FlameMap<List<Object>, ModelParameters> modelAggregator = new FlameMap<>(
            new ModelAggregator(),
            List.class
    );

    final FlameMap<List<Object>, List<Object>> filterTrainer = new FlameMap<List<Object>, List<Object>>(
            new TrainOrderingFilter(),
            List.class
    );


    final FlameMap<List<Object>, List<Object>> filterModelParameters = new FlameMap<List<Object>, List<Object>>(
            new ModelParametersFilter(),
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
        return tfIdfObject.topics() != null ? Stream.of(tfIdfObject) : Stream.of();
      }
    }, TfIdfObject.class);

    final FlameMap<List<Object>, List<Object>> filterPredictBatch = new FlameMap<>(new Function<List<Object>, Stream<List<Object>>>() {
      @Override
      public Stream<List<Object>> apply(List<Object> tfIdfObjects) {
        return tfIdfObjects.size() >= 5 ? Stream.of(tfIdfObjects) : Stream.of();
      }
    }, List.class);


    final FlameMap<List<Object>, List<Object>> filterResetBatch = new FlameMap<>(new Function<List<Object>, Stream<List<Object>>>() {
      @Override
      public Stream<List<Object>> apply(List<Object> tfIdfObjects) {
        return tfIdfObjects.size() >= 5 ? Stream.of(new CopyOnWriteArrayList<>()) : Stream.of(tfIdfObjects);
      }
    }, List.class);


    final FlameMap<List<DocContainer>, List<DocContainer>> filterDoc = new FlameMap<>(
            new DocContainerOrderingFilter(),
            List.class
    );

    final FlameMap<List<DocContainer>, DocContainer> filterTfIdf = new FlameMap<>(
            new TfIdfFilter(),
            List.class
    );

    final Classifier classifier = new Classifier(predictor);
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

    final TrainFilter trainFilter = new TrainFilter();
    //noinspection Convert2Lambda,Anonymous2MethodRef
    final FlameMap<List<TfIdfObject>, TrainInput> trainFilterMap = new FlameMap<>(
            trainFilter,
            List.class
    );

    final FlameMap<IdfObject, IdfObject> idfObjectCompleteFilter = new FlameMap<>(
            new IDFObjectCompleteFilter(),
            IdfObject.class
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

            .link(counterWord, groupingDoc)
            .link(groupingDoc, filterDoc)
            .link(filterDoc, idfAggregator)
            .link(idfAggregator, groupingDoc)

            .link(idfAggregator, idfObjectCompleteFilter)

            .link(idfObjectCompleteFilter, gropingTfIdf)
            .link(gropingTfIdf, filterTfIdf)
            .link(filterClassifier, sink)

            .link(filterTfIdf, filterToClassify)
            .link(filterTfIdf, filterToPredict)
            .link(filterToClassify, filterClassifier)
            .link(filterClassifier, sink)

            .link(filterToPredict, groupingPredictionBatch)
            .link(groupingPredictionBatch, filterTrainer)
            .link(filterTrainer, trainAggregator)
            .link(trainAggregator, filterResetBatch)
            .link(filterResetBatch, groupingPredictionBatch)
            .link(trainAggregator, filterPredictBatch)

            .link(filterPredictBatch, trainFilterMap)
            .link(trainFilterMap, groupingModelParameters)
            .link(groupingModelParameters, filterModelParameters)
            .link(filterModelParameters, modelAggregator)
            .link(modelAggregator, groupingModelParameters)

            .colocate(source, splitterTf, splitterWord)
            .colocate(groupingWord, filterWord, counterWord)
            .colocate(
                    groupingDoc,
                    filterDoc,
                    idfAggregator,
                    idfObjectCompleteFilter,
                    gropingTfIdf,
                    filterTfIdf,
                    sink
            )
            .colocate(groupingPredictionBatch, filterTrainer, trainAggregator)
            .colocate(groupingModelParameters, filterModelParameters, modelAggregator)

            .build(source, sink);
  }
}
