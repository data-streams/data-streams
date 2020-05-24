package com.spbsu.flamestream.example.bl.text_classifier;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.SerializablePredicate;
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
  private static final Class<Either<DocContainer, DocumentKey>> WORD_COUNTER_OR_MARKER =
          (Class<Either<DocContainer, DocumentKey>>) (Class<?>) Either.class;
  private final Vectorizer vectorizer;
  private final OnlineModel onlineModel;

  public TextClassifierGraph(Vectorizer vectorizer, OnlineModel onlineModel) {
    this.vectorizer = vectorizer;
    this.onlineModel = onlineModel;
  }

  private static class DocumentKey {
    final int number;
    final String partitioning;
    final boolean labeled;

    public DocumentKey(int number, String partitioning, boolean labeled) {
      this.number = number;
      this.partitioning = partitioning;
      this.labeled = labeled;
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
    final Operator<TfObject> source = textDocumentInput
            .new MapBuilder<TfObject>(TfObject.class, text -> Stream.of(TfObject.ofText(text)))
            .hash(documentHashing).build();
    final Operator.LabelSpawn<TfObject, DocumentKey> labeled = source.filter(TfObject::labeled)
            .spawnLabel(DocumentKey.class, tfObject ->
                    new DocumentKey(tfObject.number(), tfObject.partitioning(), tfObject.labeled())
            );
    final Operator.LabelSpawn<TfObject, DocumentKey> unlabeled = source.filter(tfObject -> !tfObject.labeled())
            .spawnLabel(DocumentKey.class, tfObject ->
                    new DocumentKey(tfObject.number(), tfObject.partitioning(), tfObject.labeled())
            );
    final Operator.Input<TfObject> tfObjects = new Operator.Input<>(TfObject.class).link(labeled).link(unlabeled);
    final Operator<WordEntry> wordEntries = tfObjects.flatMap(
            WordEntry.class,
            tfObject -> tfObject.counts().keySet().stream().map(integer -> new WordEntry(
                    integer,
                    tfObject.document(),
                    tfObject.counts().size(),
                    tfObject.partitioning(),
                    tfObject.labeled()
            ))
    );
    final Set<Operator.LabelSpawn<?, ?>> unlabeledLabels = Collections.singleton(unlabeled);
    final Operator<WordCounter> wordCounters = new Operator.Keyed<>(wordEntries.newKeyedBuilder(WordEntry::word))
            .statefulMap(WordCounter.class, (WordEntry wordEntry, WordCounter previous) -> {
              final WordCounter updated = previous == null ? new WordCounter(wordEntry, 1) : new WordCounter(
                      wordEntry,
                      previous.count() + 1
              );
              return new Tuple2<>(updated, updated);
            });
    final Operator.Input<DocContainer> scatteredTfsAndWordCounters =
            new Operator.Input<>(DocContainer.class).link(tfObjects).link(wordCounters);

    final Operator<TfIdfObject> tfIdfObjects = new Operator.Input<>(TfIdfObject.class)
            .link(
                    tfIdfObjects(
                            scatteredTfsAndWordCounters
                                    .filter(DocContainer::labeled)
                                    .labelMarkers(labeled, Operator.Hashing.Special.Broadcast),
                            labeled
                    )
            )
            .link(
                    tfIdfObjects(
                            scatteredTfsAndWordCounters
                                    .filter(docContainer -> !docContainer.labeled())
                                    .labelMarkers(unlabeled, new Operator.Hashing<Object>() {
                                      @Override
                                      public Set<Operator.LabelSpawn<?, ?>> labels() {
                                        return unlabeledLabels;
                                      }

                                      @Override
                                      public int applyAsInt(Object aVoid) {
                                        return 0;
                                      }
                                    }),
                            unlabeled
                    )
            );
    final Classifier classifier = new Classifier(vectorizer, onlineModel);
    final Operator<Prediction> predictions = new Operator.Keyed<>(
            tfIdfObjects.newKeyedBuilder().hash(Operator.Hashing.Special.PostBroadcast)
    ).statefulFlatMap(Prediction.class, (TfIdfObject tfIdfObject, ClassifierState classifierState) -> {
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
    });
    return Materializer.materialize(new Flow<>(textDocumentInput, predictions, __ -> classifier.init()));
  }

  private Operator<TfIdfObject> tfIdfObjects(
          Operator<Either<DocContainer, DocumentKey>> markered, Operator.LabelSpawn<?, ?> labels
  ) {
    return new Operator.Grouping<>(
            new Operator.Keyed<>(markered.newKeyedBuilder()
                    .keyLabels(Collections.singleton(labels))
                    .hash(Operator.Hashing.Special.PostBroadcast)),
            Integer.MAX_VALUE,
            true
    ).filter(grouped -> grouped.get(grouped.size() - 1).isRight()).map(TfIdfObject.class, grouped -> new TfIdfObject(
            (TfObject) grouped.get(0).left().get(),
            new IdfObject(grouped.subList(1, grouped.size() - 1)
                    .stream()
                    .map(either -> (WordCounter) either.left().get())
                    .collect(Collectors.toSet()))
    ));
  }
}
