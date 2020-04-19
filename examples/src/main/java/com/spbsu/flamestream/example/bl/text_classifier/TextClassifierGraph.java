package com.spbsu.flamestream.example.bl.text_classifier;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.example.bl.text_classifier.model.ClassifierState;
import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordEntry;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.Classifier;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.OnlineModel;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.labels.Flow;
import com.spbsu.flamestream.example.labels.Materializer;
import com.spbsu.flamestream.example.labels.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.util.Either;
import scala.util.Left;
import scala.util.Right;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TextClassifierGraph implements Supplier<Graph> {
  private final static Logger LOG = LoggerFactory.getLogger(TextClassifierGraph.class);

  @SuppressWarnings("unchecked")
  private static final Class<Either<WordCounter, DocumentKey>> EITHER_WORD_COUNTER_OR_MARKER =
          (Class<Either<WordCounter, DocumentKey>>) (Class<?>) Either.class;
  private final Vectorizer vectorizer;
  private final OnlineModel onlineModel;

  TextClassifierGraph(Vectorizer vectorizer, OnlineModel onlineModel) {
    this.vectorizer = vectorizer;
    this.onlineModel = onlineModel;
  }

  private static class DocumentKey {
    final int number;
    final String partitioning;

    public DocumentKey(int number, String partitioning) {
      this.number = number;
      this.partitioning = partitioning;
    }

    @Override
    public String toString() {
      return "(" + number + ", " + partitioning + ")";
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof DocumentKey) {
        return number == ((DocumentKey) obj).number;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return partitioning.hashCode();
    }
  }

  @Override
  public Graph get() {
    final Operator.Hashing<TextDocument> documentHashing = text -> text.partitioning().hashCode();
    final Operator.Input<TextDocument> textDocumentInput = new Operator.Input<>(TextDocument.class);
    final Operator.LabelSpawn<TfObject, DocumentKey> documents = textDocumentInput
            .new MapBuilder<TfObject>(TfObject.class, text -> Stream.of(TfObject.ofText(text)))
            .hash(documentHashing).build()
            .spawnLabel(DocumentKey.class, tfObject -> new DocumentKey(tfObject.number(), tfObject.partitioning()));
    final Operator<WordEntry> wordEntries = documents.flatMap(
            WordEntry.class,
            tfObject -> tfObject.counts().keySet().stream().map(integer -> new WordEntry(
                    integer,
                    tfObject.document(),
                    tfObject.counts().size(),
                    tfObject.partitioning()
            ))
    );
    final Set<Operator.LabelSpawn<?, ?>> labels = Collections.singleton(documents);
    final Operator<Either<WordCounter, DocumentKey>> wordCounters =
            new Operator.Keyed<>(wordEntries.newKeyedBuilder(WordEntry::word)).statefulMap(
                    WordCounter.class,
                    (WordEntry wordEntry, WordCounter previous) -> {
                      final WordCounter updated = previous == null ? new WordCounter(wordEntry, 1) : new WordCounter(
                              wordEntry,
                              previous.count() + 1
                      );
                      return new Tuple2<>(updated, updated);
                    }
            ).new MapBuilder<Either<WordCounter, DocumentKey>>(
                    EITHER_WORD_COUNTER_OR_MARKER,
                    wordCounter -> Stream.of(new Left<>(wordCounter))
            ).hash(new Operator.Hashing<>() {
              @Override
              public Set<Operator.LabelSpawn<?, ?>> labels() {
                return labels;
              }

              @Override
              public int applyAsInt(WordCounter wordCounter) {
                return 0;
              }
            }).build();
    final Operator.Input<Either<WordCounter, DocumentKey>> wordCounterOrMarker =
            new Operator.Input<>(EITHER_WORD_COUNTER_OR_MARKER, labels);
    wordCounterOrMarker.link(wordCounters);
    wordCounterOrMarker.link(wordCounters.labelMarkers(documents).map(EITHER_WORD_COUNTER_OR_MARKER, Right::new));
    final Operator.Hashing<Object> labelsHashing = new Operator.Hashing<>() {
      @Override
      public Set<Operator.LabelSpawn<?, ?>> labels() {
        return labels;
      }

      @Override
      public int applyAsInt(Object aVoid) {
        return 0;
      }
    };
    final Operator<IdfObject> idfObjects = new Operator.Grouping<>(
            new Operator.Keyed<>(wordCounterOrMarker.newKeyedBuilder().keyLabels(labels).hash(labelsHashing)),
            Integer.MAX_VALUE,
            true
    ).filter(grouped -> grouped.get(grouped.size() - 1).isRight()).map(IdfObject.class, grouped ->
            new IdfObject(grouped.subList(0, grouped.size() - 1)
                    .stream()
                    .map(either -> either.left().get())
                    .collect(Collectors.toSet()))
    );
    final Operator.Input<DocContainer> tfsAndIdfs = new Operator.Input<>(DocContainer.class, labels);
    tfsAndIdfs.link(idfObjects);
    tfsAndIdfs.link(documents);
    final Operator<TfIdfObject> tfIdfs = new Operator.Grouping<>(
            new Operator.Keyed<>(tfsAndIdfs.newKeyedBuilder().keyLabels(labels).hash(labelsHashing)),
            2,
            true
    ).filter(docContainers -> docContainers.size() == 2).map(TfIdfObject.class, docContainers ->
            new TfIdfObject((TfObject) docContainers.get(0), (IdfObject) docContainers.get(1))
    );
    final Operator.Input<TfIdfObject> classifierInputs = new Operator.Input<>(TfIdfObject.class);
    classifierInputs.link(tfIdfs.new MapBuilder<TfIdfObject>(
            TfIdfObject.class,
            (TfIdfObject tfIdf) -> tfIdf.label() != null ? Stream.of(tfIdf) : Stream.empty()
    ).hash(Operator.Hashing.Special.Broadcast).build());
    classifierInputs.link(tfIdfs.filter(tfIdfObject -> tfIdfObject.label() == null));
    final Classifier classifier = new Classifier(vectorizer, onlineModel);
    final Operator<Prediction> predictions = new Operator.Keyed<>(
            classifierInputs.newKeyedBuilder().hash(Operator.Hashing.Special.PostBroadcast)
    ).statefulFlatMap(
            Prediction.class,
            ((TfIdfObject tfIdfObject, ClassifierState classifierState) -> {
              if (tfIdfObject.label() == null) {
                if (classifierState == null) {
                  LOG.warn("Cannot process doc: {}. Empty model.", tfIdfObject.document());
                  return new Tuple2<>(null, Stream.empty());
                }
                return new Tuple2<>(
                        classifierState,
                        Stream.of(new Prediction(tfIdfObject, classifier.predict(classifierState, tfIdfObject)))
                );
              } else {
                return new Tuple2<>(
                        classifier.step(classifierState, tfIdfObject),
                        Stream.of(new Prediction(tfIdfObject, new Topic[]{}))
                );
              }
            })
    );
    return Materializer.materialize(new Flow<>(textDocumentInput, predictions, __ -> classifier.init()));
  }
}
