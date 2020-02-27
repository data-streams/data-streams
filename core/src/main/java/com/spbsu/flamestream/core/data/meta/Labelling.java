package com.spbsu.flamestream.core.data.meta;

public interface Labelling<
        Presence extends Labelling.Presence<Labels>,
        Labels extends Labelling.Labels<Labels>,
        InUse extends Labelling.InUse
        > {
  interface Presence<Labels> {
    boolean test(Labels labels);
  }

  interface Labels<Labels> {
    Labels added(Label<?> label);
  }

  interface InUse {
    <L> Label<L> take(L label);

    void free(Label<?> label);
  }

  Labels newLabels();

  InUse newInUse();
}
